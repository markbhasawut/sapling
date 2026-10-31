/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

//! A fake MySQL server that speaks just enough of the protocol for
//! mysql_async to connect and run text queries, and answers each query as
//! the test says. It lets tests see how real driver and server errors travel
//! through the stores' code without a database server.

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex;

use anyhow::Result;
use sql::mysql_async::OptsBuilder;
use sql::mysql_async::Pool;
use sql::mysql_async::PoolOpts;
use sql::mysql_async::consts::CapabilityFlags;
use sql_common::mysql::ConnectionStats;
use sql_common::mysql::OssConnection;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::task::JoinHandle;
use tokio::task::JoinSet;

use crate::Connection;

const COM_QUIT: u8 = 0x01;
const COM_QUERY: u8 = 0x03;

const SERVER_STATUS_AUTOCOMMIT: u16 = 0x0002;
const UTF8MB4_GENERAL_CI: u8 = 45;

/// How the server answers a query.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reply {
    /// Success, with one row affected.
    Ok,
    /// An error with this code.
    Error(u16),
    /// Closes the connection without answering.
    Close,
}

type Answer = dyn Fn(&str) -> Reply + Send + Sync;

/// A running fake server. It stops when dropped.
pub struct FakeMysqld {
    addr: SocketAddr,
    queries: Arc<Mutex<Vec<String>>>,
    task: JoinHandle<()>,
}

impl FakeMysqld {
    /// Starts a server on a free local port that accepts any user and
    /// password, and answers every query with what `answer` returns for it.
    /// Other commands succeed.
    pub async fn start(answer: impl Fn(&str) -> Reply + Send + Sync + 'static) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let queries = Arc::new(Mutex::new(Vec::new()));
        let answer: Arc<Answer> = Arc::new(answer);
        let task = tokio::spawn({
            let queries = queries.clone();
            async move {
                // Dropped with the accept loop, which ends every connection.
                let mut connections = JoinSet::new();
                let mut id = 0;
                while let Ok((stream, _)) = listener.accept().await {
                    id += 1;
                    connections.spawn(serve(stream, id, answer.clone(), queries.clone()));
                }
            }
        });
        Ok(Self {
            addr,
            queries,
            task,
        })
    }

    /// Where the server listens.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// The queries received so far, in order.
    pub fn queries(&self) -> Vec<String> {
        self.queries.lock().expect("lock poisoned").clone()
    }

    /// A new pool that connects to this server.
    pub fn pool(&self) -> Pool {
        let opts = OptsBuilder::default()
            .ip_or_hostname(self.addr.ip().to_string())
            .tcp_port(self.addr.port())
            .user(Some("mononoke"))
            .pass(Some("secret"))
            .db_name(Some("mononoke"))
            .prefer_socket(false)
            // Given, so that connecting runs no query.
            .max_allowed_packet(Some(16 << 20))
            .wait_timeout(Some(28_800))
            .pool_opts(PoolOpts::default().with_reset_connection(false));
        Pool::new(opts)
    }

    /// A connection as the stores use, over a new fixed pool to this server.
    pub fn connection(&self) -> Connection {
        let stats = Arc::new(ConnectionStats::new("fake_mysqld".to_string()));
        Connection {
            inner: OssConnection::new(self.pool(), stats).into(),
            shard_name: "fake_mysqld".to_string(),
        }
    }
}

impl Drop for FakeMysqld {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn serve(
    mut stream: TcpStream,
    connection_id: u32,
    answer: Arc<Answer>,
    queries: Arc<Mutex<Vec<String>>>,
) -> io::Result<()> {
    write_packet(&mut stream, 0, &handshake(connection_id)).await?;
    // Any credentials will do.
    let (seq, _response) = read_packet(&mut stream).await?;
    write_packet(&mut stream, seq.wrapping_add(1), &ok_packet(0)).await?;
    loop {
        let (seq, payload) = read_packet(&mut stream).await?;
        let reply = match payload.split_first() {
            Some((&COM_QUERY, query)) => {
                let query = String::from_utf8_lossy(query).into_owned();
                let reply = answer(&query);
                queries.lock().expect("lock poisoned").push(query);
                reply
            }
            Some((&COM_QUIT, _)) => return Ok(()),
            _ => Reply::Ok,
        };
        let packet = match reply {
            Reply::Ok => ok_packet(1),
            Reply::Error(code) => err_packet(code),
            Reply::Close => return Ok(()),
        };
        write_packet(&mut stream, seq.wrapping_add(1), &packet).await?;
    }
}

/// A protocol 10 handshake that asks for mysql_native_password.
fn handshake(connection_id: u32) -> Vec<u8> {
    let capabilities = (CapabilityFlags::CLIENT_LONG_PASSWORD
        | CapabilityFlags::CLIENT_LONG_FLAG
        | CapabilityFlags::CLIENT_CONNECT_WITH_DB
        | CapabilityFlags::CLIENT_PROTOCOL_41
        | CapabilityFlags::CLIENT_TRANSACTIONS
        | CapabilityFlags::CLIENT_SECURE_CONNECTION
        | CapabilityFlags::CLIENT_MULTI_STATEMENTS
        | CapabilityFlags::CLIENT_MULTI_RESULTS
        | CapabilityFlags::CLIENT_PLUGIN_AUTH)
        .bits();
    let mut packet = vec![10];
    packet.extend_from_slice(b"8.0.36-fake\0");
    packet.extend_from_slice(&connection_id.to_le_bytes());
    // The scramble: 8 bytes here, 12 and a NUL below.
    packet.extend_from_slice(b"abcdefgh\0");
    packet.extend_from_slice(&(capabilities as u16).to_le_bytes());
    packet.push(UTF8MB4_GENERAL_CI);
    packet.extend_from_slice(&SERVER_STATUS_AUTOCOMMIT.to_le_bytes());
    packet.extend_from_slice(&((capabilities >> 16) as u16).to_le_bytes());
    packet.push(21);
    packet.extend_from_slice(&[0; 10]);
    packet.extend_from_slice(b"ijklmnopqrst\0");
    packet.extend_from_slice(b"mysql_native_password\0");
    packet
}

fn ok_packet(affected_rows: u8) -> Vec<u8> {
    // Header, affected rows, last insert id, status flags, warnings.
    let mut packet = vec![0x00, affected_rows, 0x00];
    packet.extend_from_slice(&SERVER_STATUS_AUTOCOMMIT.to_le_bytes());
    packet.extend_from_slice(&[0, 0]);
    packet
}

fn err_packet(code: u16) -> Vec<u8> {
    let mut packet = vec![0xff];
    packet.extend_from_slice(&code.to_le_bytes());
    packet.extend_from_slice(b"#HY000");
    packet.extend_from_slice(format!("fake error {code}").as_bytes());
    packet
}

async fn read_packet(stream: &mut TcpStream) -> io::Result<(u8, Vec<u8>)> {
    let mut header = [0; 4];
    stream.read_exact(&mut header).await?;
    let len = u32::from_le_bytes([header[0], header[1], header[2], 0]);
    let mut payload = vec![0; len as usize];
    stream.read_exact(&mut payload).await?;
    Ok((header[3], payload))
}

async fn write_packet(stream: &mut TcpStream, seq: u8, payload: &[u8]) -> io::Result<()> {
    let len = u32::try_from(payload.len()).expect("small packets only");
    let mut packet = len.to_le_bytes()[..3].to_vec();
    packet.push(seq);
    packet.extend_from_slice(payload);
    stream.write_all(&packet).await
}
