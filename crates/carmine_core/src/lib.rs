pub mod collection;
pub mod error;
pub mod key;
pub mod store;
pub mod types;
pub mod validation;
pub mod value;
pub use error::Result;
pub use types::Number;

#[cfg(test)]
mod tests;
