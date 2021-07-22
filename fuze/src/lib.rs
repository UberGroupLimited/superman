//! Fuzes are mechanisms to wait for a single signal which can be checked at any time.
//!
//! This crate has an Async-std (default) and a Tokio implementation:
//!
//! ## Async-std
//!
//! ```toml
//! fuze = "3.0.0"
//! ```
//!
//! ## Tokio
//!
//! ```toml
//! fuze = { version = "3.0.0", default-features = false, features = ["tokio"] }
//! ```
//!
//! Both implementations can co-exist (both features can be enabled at the same time) and the API
//! is identical, but the top-level re-export will prefer the async-std version. Disabling both
//! features will not compile.

#[cfg(feature = "async-std")]
pub mod async_std;

#[cfg(feature = "tokio")]
pub mod tokio;

cfg_if::cfg_if! {
	if #[cfg(feature = "async-std")] {
		pub use crate::async_std::Fuze;
	} else if #[cfg(feature = "tokio")] {
		pub use crate::tokio::Fuze;
	} else {
		compile_error!("At least one of tokio or async-std must be enabled");
	}
}
