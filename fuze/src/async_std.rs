//! The async-std implementation of a Fuze.

use async_std::channel::{bounded, Receiver, Sender};
use std::fmt;

/// A mechanism to wait for a single signal which can be checked at any time.
///
/// A Fuze can be checked synchronously, and will show up as burnt only once it’s been burnt. It
/// can be awaited while it’s unburnt, and it can be Cloned and used without `mut` at any time.
///
/// Useful for exit conditions and as a one-off no-payload channel.
///
/// # Example
///
/// Basic usage:
///
/// ```
/// # use fuze::async_std::Fuze;
/// use async_std::task;
/// use std::time::Duration;
///
/// let f1 = Fuze::new();
/// assert_eq!(f1.burnt(), false);
///
/// let f2 = f1.clone();
/// task::block_on(async move {
///		assert_eq!(f2.burnt(), false);
///
///		let f3 = f2.clone();
///		task::spawn(async move {
///			assert_eq!(f3.burnt(), false);
///
///			task::sleep(Duration::from_secs(1)).await;
///
///			// this first burn unblocks anything that wait()s on the fuze:
///			f3.burn();
///
///			assert_eq!(f3.burnt(), true);
///
///			// this second burn does nothing:
///			f3.burn();
///
///			assert_eq!(f3.burnt(), true);
///		});
///
///		// this call will block until the f3.burn() call above:
///		println!("first");
///		f2.wait().await;
///		assert_eq!(f2.burnt(), true);
///
///		// now that the fuze is burnt, this call returns immediately:
///		println!("second");
///		f2.wait().await;
/// });
///
///	assert_eq!(f1.burnt(), true);
/// ```
#[derive(Clone)]
pub struct Fuze {
	s: Sender<()>,
	r: Receiver<()>,
}

impl fmt::Debug for Fuze {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("Fuze")
			.field("burnt", &self.burnt())
			.finish()
	}
}

impl Default for Fuze {
	fn default() -> Self {
		Self::new()
	}
}

impl Fuze {
	/// Creates a new unburnt fuze.
	///
	/// # Examples
	///
	/// Basic usage:
	///
	/// ```
	/// # use fuze::async_std::Fuze;
	/// let fuze = Fuze::new();
	///	assert_eq!(fuze.burnt(), false);
	/// fuze.burn();
	///	assert_eq!(fuze.burnt(), true);
	/// ```
	pub fn new() -> Self {
		let (s, r) = bounded(1);
		Self { s, r }
	}

	/// Burns the fuze, unblocking everything that is waiting on it.
	pub fn burn(&self) {
		if !self.s.is_closed() {
			self.s.close();
		}
	}

	/// Checks whether the fuze is burnt or not.
	pub fn burnt(&self) -> bool {
		self.s.is_closed()
	}

	/// Blocks (async-ly) until the fuze is burnt, then never blocks again.
	pub async fn wait(&self) {
		if !self.s.is_closed() {
			self.r.recv().await.ok();
		}
	}
}
