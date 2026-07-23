pub mod error;
pub mod panic_recovery;

pub use error::{CatalogError, DharnessError, ProcedureError};
pub type Result<T> = std::result::Result<T, DharnessError>;
