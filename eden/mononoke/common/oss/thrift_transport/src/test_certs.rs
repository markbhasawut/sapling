/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

//! Certificates for tests: a CA, and certificates it signs for servers and
//! clients, valid or not.

use openssl::asn1::Asn1Integer;
use openssl::asn1::Asn1Time;
use openssl::bn::BigNum;
use openssl::ec::EcGroup;
use openssl::ec::EcKey;
use openssl::hash::MessageDigest;
use openssl::nid::Nid;
use openssl::pkey::PKey;
use openssl::pkey::Private;
use openssl::ssl::SslConnector;
use openssl::ssl::SslMethod;
use openssl::ssl::SslVersion;
use openssl::x509::X509;
use openssl::x509::X509Builder;
use openssl::x509::X509NameBuilder;
use openssl::x509::extension::BasicConstraints;
use openssl::x509::extension::ExtendedKeyUsage;
use openssl::x509::extension::KeyUsage;
use openssl::x509::extension::SubjectAlternativeName;

use crate::ClientTls;
use crate::ServerTls;

pub(crate) struct Ca {
    certificate: X509,
    key: PKey<Private>,
}

/// A certificate and its key, as PEM.
pub(crate) struct Issued {
    pub(crate) certificate: Vec<u8>,
    pub(crate) key: Vec<u8>,
}

fn new_key() -> PKey<Private> {
    let group = EcGroup::from_curve_name(Nid::X9_62_PRIME256V1).unwrap();
    PKey::from_ec_key(EcKey::generate(&group).unwrap()).unwrap()
}

fn serial() -> Asn1Integer {
    let mut serial = BigNum::new().unwrap();
    serial
        .rand(64, openssl::bn::MsbOption::MAYBE_ZERO, false)
        .unwrap();
    serial.to_asn1_integer().unwrap()
}

/// Validity from `from` days to `to` days from now.
pub(crate) struct Validity {
    from: i64,
    to: i64,
}

pub(crate) const VALID: Validity = Validity { from: -1, to: 1 };
pub(crate) const EXPIRED: Validity = Validity { from: -3, to: -2 };

fn days(days: i64) -> Asn1Time {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    Asn1Time::from_unix(now + days * 24 * 60 * 60).unwrap()
}

impl Ca {
    pub(crate) fn new(name: &str) -> Self {
        let key = new_key();
        let mut subject = X509NameBuilder::new().unwrap();
        subject.append_entry_by_text("CN", name).unwrap();
        let subject = subject.build();
        let mut builder = X509Builder::new().unwrap();
        builder.set_version(2).unwrap();
        builder.set_serial_number(&serial()).unwrap();
        builder.set_subject_name(&subject).unwrap();
        builder.set_issuer_name(&subject).unwrap();
        builder.set_pubkey(&key).unwrap();
        builder.set_not_before(&days(-1)).unwrap();
        builder.set_not_after(&days(30)).unwrap();
        builder
            .append_extension(BasicConstraints::new().critical().ca().build().unwrap())
            .unwrap();
        builder
            .append_extension(
                KeyUsage::new()
                    .critical()
                    .key_cert_sign()
                    .crl_sign()
                    .build()
                    .unwrap(),
            )
            .unwrap();
        builder.sign(&key, MessageDigest::sha256()).unwrap();
        Self {
            certificate: builder.build(),
            key,
        }
    }

    pub(crate) fn pem(&self) -> Vec<u8> {
        self.certificate.to_pem().unwrap()
    }

    fn issue(
        &self,
        subject: &[(&str, &str)],
        validity: Validity,
        server: bool,
        add_names: impl FnOnce(&mut SubjectAlternativeName),
    ) -> Issued {
        let key = new_key();
        let mut name = X509NameBuilder::new().unwrap();
        for (field, value) in subject {
            name.append_entry_by_text(field, value).unwrap();
        }
        let name = name.build();
        let mut builder = X509Builder::new().unwrap();
        builder.set_version(2).unwrap();
        builder.set_serial_number(&serial()).unwrap();
        builder.set_subject_name(&name).unwrap();
        builder
            .set_issuer_name(self.certificate.subject_name())
            .unwrap();
        builder.set_pubkey(&key).unwrap();
        builder.set_not_before(&days(validity.from)).unwrap();
        builder.set_not_after(&days(validity.to)).unwrap();
        let mut usage = ExtendedKeyUsage::new();
        if server {
            usage.server_auth();
        } else {
            usage.client_auth();
        }
        builder.append_extension(usage.build().unwrap()).unwrap();
        let mut names = SubjectAlternativeName::new();
        add_names(&mut names);
        let names = names
            .build(&builder.x509v3_context(Some(&self.certificate), None))
            .unwrap();
        builder.append_extension(names).unwrap();
        builder.sign(&self.key, MessageDigest::sha256()).unwrap();
        Issued {
            certificate: builder.build().to_pem().unwrap(),
            key: key.private_key_to_pem_pkcs8().unwrap(),
        }
    }

    /// A server certificate for `localhost` and the loopback addresses.
    pub(crate) fn server(&self) -> Issued {
        self.server_for(&["localhost"], &["127.0.0.1", "::1"], VALID)
    }

    pub(crate) fn expired_server(&self) -> Issued {
        self.server_for(&["localhost"], &["127.0.0.1", "::1"], EXPIRED)
    }

    pub(crate) fn server_for(&self, dns: &[&str], ips: &[&str], validity: Validity) -> Issued {
        self.issue(&[("CN", "server")], validity, true, |names| {
            for name in dns {
                names.dns(name);
            }
            for ip in ips {
                names.ip(ip);
            }
        })
    }

    /// A client certificate as the private CA issues them:
    /// `CN=<user>,O=mononoke-local` with SAN `URI:user:<user>`.
    pub(crate) fn user(&self, user: &str) -> Issued {
        self.client(&[("CN", user), ("O", "mononoke-local")], &format!("user:{user}"), VALID)
    }

    pub(crate) fn expired_user(&self, user: &str) -> Issued {
        self.client(&[("CN", user), ("O", "mononoke-local")], &format!("user:{user}"), EXPIRED)
    }

    pub(crate) fn client(&self, subject: &[(&str, &str)], uri: &str, validity: Validity) -> Issued {
        self.issue(subject, validity, false, |names| {
            names.uri(uri);
        })
    }

    pub(crate) fn server_tls(&self) -> ServerTls {
        let issued = self.server();
        ServerTls::from_pem(&self.pem(), &issued.certificate, &issued.key).unwrap()
    }

    pub(crate) fn client_tls(&self, issued: &Issued) -> ClientTls {
        ClientTls::from_pem(&self.pem(), &issued.certificate, &issued.key).unwrap()
    }
}

/// A TLS client with none of the transport's requirements: trusts `ca`,
/// presents `issued` if given, and offers ALPN `h2` and TLS 1.3 only if
/// asked.
pub(crate) fn lax_client_tls(
    ca: &Ca,
    issued: Option<&Issued>,
    alpn: bool,
    max_version: SslVersion,
) -> ClientTls {
    let mut builder = SslConnector::builder(SslMethod::tls_client()).unwrap();
    builder.set_max_proto_version(Some(max_version)).unwrap();
    builder
        .cert_store_mut()
        .add_cert(X509::from_pem(&ca.pem()).unwrap())
        .unwrap();
    if let Some(issued) = issued {
        builder
            .set_certificate(&X509::from_pem(&issued.certificate).unwrap())
            .unwrap();
        builder
            .set_private_key(&PKey::private_key_from_pem(&issued.key).unwrap())
            .unwrap();
    }
    if alpn {
        builder.set_alpn_protos(b"\x02h2").unwrap();
    }
    ClientTls::from_connector(builder.build())
}
