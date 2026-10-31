/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

//! Stub implementation of AuthenticatedIdentity for OSS builds.
//! This provides the minimal structure needed for the MononokeIdentity enum
//! to compile in OSS mode, but the functionality is not available.

use anyhow::Result;
use anyhow::bail;
#[cfg(fbcode_build)]
use async_trait::async_trait;
use itertools::Itertools;
use openssl::x509::X509;

use crate::identity::ClientCategory;
use crate::identity::MononokeIdentity;
use crate::identity::MononokeIdentitySet;
use crate::identity::MononokeIdentitySetExt;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthenticatedIdentity {
    pub identity: Identity,
    pub attributes: Vec<Attribute>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Identity {
    pub id_type: String,
    pub id_data: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Attribute {
    pub identifier: AttributeKey,
    pub value: IdentityAttribute,
    pub val: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AttributeKey {
    pub attributeName: String,
    pub attributeNamespace: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IdentityAttribute {
    pub attributeValue: String,
}

impl MononokeIdentity {
    pub fn reviewer_identities(_username: &str) -> MononokeIdentitySet {
        MononokeIdentitySet::new()
    }

    pub fn try_from_json_encoded(_: &str) -> Result<MononokeIdentitySet> {
        bail!("Decoding from JSON is not yet implemented for MononokeIdentity")
    }

    pub fn try_from_legacy_encoded(_: &str) -> Result<MononokeIdentitySet> {
        bail!(
            "Decoding legacy scm_forwarded_identities is not yet implemented for MononokeIdentity"
        )
    }

    pub fn try_from_thrift_compact_bytes(_: &[u8]) -> Result<MononokeIdentitySet> {
        bail!(
            "Decoding compact-encoded AuthenticatedIdentity is not supported for MononokeIdentity in OSS builds"
        )
    }

    /// The identities of a client certificate: `X509_SUBJECT_NAME:<subject>`,
    /// plus `USER:<name>` for each subject alternative name `URI:user:<name>`,
    /// which is how a private CA can give its users a unixname, as Meta's
    /// certificates do.
    pub fn try_from_x509(cert: &X509) -> Result<MononokeIdentitySet> {
        let subject_vec: Result<Vec<_>> = cert
            .subject_name()
            .entries()
            .map(|entry| {
                Ok(format!(
                    "{}={}",
                    entry.object().nid().short_name()?,
                    escape_subject_value(&entry.data().as_utf8()?)
                ))
            })
            .collect();
        let subject_name = subject_vec?.as_slice().join(",");

        let mut idents = MononokeIdentitySet::new();
        idents.insert(MononokeIdentity::from_legacy_type_data(
            "X509_SUBJECT_NAME",
            subject_name,
        ));
        for name in cert.subject_alt_names().into_iter().flatten() {
            if let Some(user) = name.uri().and_then(|uri| uri.strip_prefix("user:")) {
                if is_valid_unixname(user) {
                    idents.insert(MononokeIdentity::from_legacy_type_data("USER", user));
                }
            }
        }
        Ok(idents)
    }
}

/// Escape `\` and `,` in a subject entry's value, so that two different
/// subjects never join into the same identity: unescaped, a single CN of
/// `alice,O=example` would read as CN `alice` and O `example`.
fn escape_subject_value(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for c in value.chars() {
        if c == '\\' || c == ',' {
            escaped.push('\\');
        }
        escaped.push(c);
    }
    escaped
}

/// Whether a `user:` SAN names something that can be a unixname. Names that
/// are all digits are left out, as `Metadata::unix_name` takes those for FBIDs.
fn is_valid_unixname(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && !name.chars().all(|c| c.is_ascii_digit())
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-')
}

impl MononokeIdentitySetExt for MononokeIdentitySet {
    fn likely_an_agent(&self) -> bool {
        false
    }

    fn is_proxygen_test_identity(&self) -> bool {
        false
    }

    fn hostprefix(&self) -> Option<&str> {
        None
    }

    fn hostname(&self) -> Option<&str> {
        None
    }

    fn username(&self) -> Option<&str> {
        self.iter()
            .find(|identity| identity.id_type() == "USER")
            .map(|identity| identity.id_data())
    }

    fn crewmate(&self) -> Option<&str> {
        None
    }

    fn sandcastle_job_id(&self) -> Option<&str> {
        None
    }

    fn on_demand_type(&self) -> Option<&str> {
        None
    }

    fn main_client_identity(&self, _sandcastle_alias: Option<&str>) -> String {
        String::from("PLACEHOLDER_CLIENT_IDENTITY")
    }

    fn client_category(&self, _sandcastle_alias: Option<&str>) -> ClientCategory {
        ClientCategory::Unknown
    }

    fn to_string(&self) -> String {
        self.iter().map(ToString::to_string).join(",")
    }

    fn identity_type_filtered_concat(&self, _id_type: &str) -> Option<String> {
        None
    }
}

#[cfg(test)]
mod tests {
    use mononoke_macros::mononoke;
    use openssl::asn1::Asn1Time;
    use openssl::hash::MessageDigest;
    use openssl::pkey::PKey;
    use openssl::rsa::Rsa;
    use openssl::x509::X509Builder;
    use openssl::x509::X509NameBuilder;
    use openssl::x509::extension::SubjectAlternativeName;

    use super::*;

    fn cert(subject: &[(&str, &str)], uris: &[&str]) -> X509 {
        let key = PKey::from_rsa(Rsa::generate(2048).unwrap()).unwrap();
        let mut name = X509NameBuilder::new().unwrap();
        for (field, value) in subject {
            name.append_entry_by_text(field, value).unwrap();
        }
        let name = name.build();
        let mut builder = X509Builder::new().unwrap();
        builder.set_version(2).unwrap();
        builder.set_subject_name(&name).unwrap();
        builder.set_issuer_name(&name).unwrap();
        builder.set_pubkey(&key).unwrap();
        builder
            .set_not_before(&Asn1Time::days_from_now(0).unwrap())
            .unwrap();
        builder
            .set_not_after(&Asn1Time::days_from_now(1).unwrap())
            .unwrap();
        if !uris.is_empty() {
            let mut san = SubjectAlternativeName::new();
            for uri in uris {
                san.uri(uri);
            }
            let san = san.build(&builder.x509v3_context(None, None)).unwrap();
            builder.append_extension(san).unwrap();
        }
        builder.sign(&key, MessageDigest::sha256()).unwrap();
        builder.build()
    }

    fn typed(idents: &MononokeIdentitySet) -> Vec<String> {
        idents.iter().map(|id| id.to_string()).sorted().collect()
    }

    #[mononoke::test]
    fn test_subject_identity() {
        let idents =
            MononokeIdentity::try_from_x509(&cert(&[("CN", "alice"), ("O", "example")], &[]))
                .unwrap();
        assert_eq!(typed(&idents), vec!["X509_SUBJECT_NAME:CN=alice,O=example"]);
        assert_eq!(idents.username(), None);
    }

    #[mononoke::test]
    fn test_subject_values_are_escaped() {
        let forged =
            MononokeIdentity::try_from_x509(&cert(&[("CN", "alice,O=example")], &[])).unwrap();
        assert_eq!(
            typed(&forged),
            vec![r"X509_SUBJECT_NAME:CN=alice\,O=example"]
        );
        let backslash = MononokeIdentity::try_from_x509(&cert(&[("CN", r"a\b")], &[])).unwrap();
        assert_eq!(typed(&backslash), vec![r"X509_SUBJECT_NAME:CN=a\\b"]);
    }

    #[mononoke::test]
    fn test_user_from_san() {
        let idents = MononokeIdentity::try_from_x509(&cert(
            &[("CN", "alice"), ("O", "example")],
            &[
                "user:alice",
                "https://example.com/",
                "user:",
                "user:1234",
                "user:a b",
            ],
        ))
        .unwrap();
        assert_eq!(
            typed(&idents),
            vec!["USER:alice", "X509_SUBJECT_NAME:CN=alice,O=example"]
        );
        assert_eq!(idents.username(), Some("alice"));
    }
}
