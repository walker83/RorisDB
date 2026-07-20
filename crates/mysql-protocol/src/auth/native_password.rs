use async_trait::async_trait;
use dashmap::DashMap;
use sha1::{Digest, Sha1};
use std::sync::Arc;

use super::{AuthError, AuthPlugin, AuthPluginType, AuthUser};

/// Credentials map: username → SHA1(SHA1(password)) (MySQL native password double-hash).
pub type Credentials = Arc<DashMap<String, Vec<u8>>>;

/// Compute SHA1(SHA1(password)) — the format MySQL stores for native password auth.
pub fn double_sha1(password: &[u8]) -> Vec<u8> {
    let hash1 = Sha1::digest(password);
    Sha1::digest(hash1).to_vec()
}

/// Create a default credentials map with root user (empty password).
/// WARNING: For testing only. Production code should use `secure_credentials()`.
pub fn default_credentials() -> Credentials {
    let creds = DashMap::new();
    creds.insert("root".to_string(), double_sha1(b""));
    Arc::new(creds)
}

/// Create credentials for production use, reading root password from
/// HARNESS_ROOT_PASSWORD env var. If not set, a random password is generated
/// and logged once at startup.
pub fn secure_credentials() -> Credentials {
    let password = match std::env::var("HARNESS_ROOT_PASSWORD") {
        Ok(p) if !p.is_empty() => p,
        _ => {
            use std::sync::OnceLock;
            static GEN_PASS: OnceLock<String> = OnceLock::new();
            GEN_PASS
                .get_or_init(|| {
                    use std::time::SystemTime;
                    let now = SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_nanos();
                    let pid = std::process::id() as u128;
                    let rand = now ^ (pid << 32) ^ (now.wrapping_mul(6364136223846793005));
                    let pass = format!("{:024x}", rand);
                    tracing::warn!(
                        "HARNESS_ROOT_PASSWORD not set — generated random root password: {} \
                         Set HARNESS_ROOT_PASSWORD env var for stable credentials.",
                        pass
                    );
                    pass
                })
                .clone()
        }
    };
    let creds = DashMap::new();
    creds.insert("root".to_string(), double_sha1(password.as_bytes()));
    Arc::new(creds)
}

pub struct NativePasswordAuth {
    credentials: Credentials,
}

impl NativePasswordAuth {
    pub fn new() -> Self {
        Self {
            credentials: default_credentials(),
        }
    }

    pub fn with_credentials(credentials: Credentials) -> Self {
        Self { credentials }
    }

    /// Compute the MySQL native password scramble:
    /// `SHA1(password) XOR SHA1(salt + SHA1(SHA1(password)))`
    #[cfg(test)]
    fn scramble_native_password(password: &[u8], salt: &[u8]) -> Vec<u8> {
        let hash1 = Sha1::digest(password);
        let hash2 = Sha1::digest(hash1);

        let mut hasher = Sha1::new();
        hasher.update(salt);
        hasher.update(&hash2);
        let mut result = hasher.finalize();

        for (r, h1) in result.iter_mut().zip(hash1.iter()) {
            *r ^= h1;
        }
        result.to_vec()
    }
}

#[async_trait]
impl AuthPlugin for NativePasswordAuth {
    fn plugin_type(&self) -> AuthPluginType {
        AuthPluginType::NativePassword
    }

    async fn authenticate(
        &self,
        username: &str,
        auth_response: &[u8],
        auth_plugin_data: &[u8],
    ) -> Result<AuthUser, AuthError> {
        if username.is_empty() {
            return Err(AuthError::Failed("Empty username".to_string()));
        }

        let stored_double_sha1 = match self.credentials.get(username) {
            Some(entry) => entry.value().clone(),
            None => {
                return Err(AuthError::Failed(format!(
                    "Access denied for user '{}'",
                    username
                )));
            }
        };

        // Empty password: client sends empty auth_response
        if auth_response.is_empty() {
            let empty_double_sha1 = double_sha1(b"");
            if stored_double_sha1 == empty_double_sha1 {
                return Ok(AuthUser {
                    username: username.to_string(),
                    hostname: None,
                    roles: vec!["public".to_string()],
                    auth_plugin: AuthPluginType::NativePassword,
                });
            }
            return Err(AuthError::Failed(format!(
                "Access denied for user '{}' (using password: NO)",
                username
            )));
        }

        // MySQL native password verification:
        //   Client sends: scramble = SHA1(pw) XOR SHA1(salt + SHA1(SHA1(pw)))
        //   Server has:   stored = SHA1(SHA1(pw))
        //   Server recovers: SHA1(pw) = scramble XOR SHA1(salt + stored)
        //   Server checks: SHA1(SHA1(pw)) == stored
        let salt = if auth_plugin_data.len() >= 20 {
            &auth_plugin_data[..20]
        } else {
            auth_plugin_data
        };

        // Recover SHA1(password) from the scramble
        let mut hasher = Sha1::new();
        hasher.update(salt);
        hasher.update(&stored_double_sha1);
        let hash_salt_stage = hasher.finalize();

        let mut recovered_sha1 = [0u8; 20];
        for (r, (s, h)) in recovered_sha1
            .iter_mut()
            .zip(auth_response.iter().zip(hash_salt_stage.iter()))
        {
            *r = s ^ h;
        }

        // Verify: SHA1(recovered) == stored_double_sha1
        let check = Sha1::digest(recovered_sha1);
        if check.as_slice() == stored_double_sha1.as_slice() {
            return Ok(AuthUser {
                username: username.to_string(),
                hostname: None,
                roles: vec!["public".to_string()],
                auth_plugin: AuthPluginType::NativePassword,
            });
        }

        Err(AuthError::Failed(format!(
            "Access denied for user '{}' (using password: YES)",
            username
        )))
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PasswordHash {
    pub hash: String,
}

impl PasswordHash {
    pub fn from_password(password: &[u8]) -> Self {
        Self {
            hash: Self::hash_password(password),
        }
    }

    fn hash_password(password: &[u8]) -> String {
        hex::encode(Sha1::digest(password))
    }

    pub fn verify(&self, password: &[u8]) -> bool {
        self.hash == Self::hash_password(password)
    }
}

impl Default for NativePasswordAuth {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_password_hash() {
        let hash = PasswordHash::from_password(b"password");
        assert!(hash.verify(b"password"));
        assert!(!hash.verify(b"wrong"));
    }

    #[test]
    fn test_scramble_roundtrip() {
        let salt = [1u8; 20];
        let password = b"test_password";

        // Client side: compute scramble
        let scrambled = NativePasswordAuth::scramble_native_password(password, &salt);

        // Server side: verify via recovery
        let stored = double_sha1(password);
        let mut hasher = Sha1::new();
        hasher.update(&salt);
        hasher.update(&stored);
        let hash_salt_stage = hasher.finalize();

        let mut recovered = [0u8; 20];
        for (r, (s, h)) in recovered
            .iter_mut()
            .zip(scrambled.iter().zip(hash_salt_stage.iter()))
        {
            *r = s ^ h;
        }

        let check = Sha1::digest(recovered);
        assert_eq!(check.as_slice(), stored.as_slice());
    }

    #[test]
    fn test_double_sha1_empty() {
        let result = double_sha1(b"");
        assert_eq!(result.len(), 20);
        // SHA1("") = da39a3ee...
        // SHA1(SHA1("")) = known value
    }

    #[test]
    fn test_default_credentials_has_root() {
        let creds = default_credentials();
        assert!(creds.contains_key("root"));
    }

    #[tokio::test]
    async fn test_authenticate_root_no_password() {
        let auth = NativePasswordAuth::new();
        let result = auth.authenticate("root", &[], &[0u8; 20]).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().username, "root");
    }

    #[tokio::test]
    async fn test_authenticate_unknown_user() {
        let auth = NativePasswordAuth::new();
        let result = auth.authenticate("unknown", &[], &[0u8; 20]).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_authenticate_with_password() {
        let creds = default_credentials();
        let password = b"secret123";
        creds.insert("testuser".to_string(), double_sha1(password));

        let auth = NativePasswordAuth::with_credentials(creds.clone());
        let salt = [42u8; 20];

        // Compute valid scramble
        let scramble = NativePasswordAuth::scramble_native_password(password, &salt);
        let result = auth.authenticate("testuser", &scramble, &salt).await;
        assert!(result.is_ok());

        // Wrong password should fail
        let wrong_scramble = NativePasswordAuth::scramble_native_password(b"wrong", &salt);
        let result = auth.authenticate("testuser", &wrong_scramble, &salt).await;
        assert!(result.is_err());
    }
}
