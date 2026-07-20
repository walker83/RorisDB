pub mod native_password;
pub mod plugin;
pub mod token;

pub use native_password::{
    Credentials, NativePasswordAuth, PasswordHash, default_credentials, double_sha1,
    secure_credentials,
};
pub use plugin::{AuthError, AuthPlugin, AuthPluginType, AuthUser};
pub use token::{JwtClaims, TokenAuth, TokenConfig, generate_jwt_token, validate_jwt_token};
