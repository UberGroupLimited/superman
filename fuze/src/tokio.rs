//! The Tokio implementation of a Fuze.

use tokio::sync::Notify;
use std::{fmt, sync::{Arc, atomic::{AtomicBool, Ordering::SeqCst}}};

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
/// # use fuze::tokio::Fuze;
/// use std::time::Duration;
/// use tokio::{runtime::Runtime, time::sleep};
/// let rt = Runtime::new().unwrap();
///
/// let f1 = Fuze::new();
/// assert_eq!(f1.burnt(), false);
///
/// let f2 = f1.clone();
/// rt.block_on(async move {
///		assert_eq!(f2.burnt(), false);
///
///		let f3 = f2.clone();
///		let t = tokio::spawn(async move {
///			assert_eq!(f3.burnt(), false);
///
///			sleep(Duration::from_secs(1)).await;
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
///
///		t.await;
/// });
///
///	assert_eq!(f1.burnt(), true);
/// ```
#[derive(Clone, Default)]
pub struct Fuze(Arc<FuzeImp>);

#[derive(Default)]
struct FuzeImp {
	n: Notify,
	b: AtomicBool,
}

impl fmt::Debug for Fuze {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("Fuze")
			.field("burnt", &self.burnt())
			.finish()
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
	/// # use fuze::tokio::Fuze;
	/// let fuze = Fuze::new();
	///	assert_eq!(fuze.burnt(), false);
	/// fuze.burn();
	///	assert_eq!(fuze.burnt(), true);
	/// ```
	pub fn new() -> Self {
		Self::default()
	}

	/// Burns the fuze, unblocking everything that is waiting on it.
	pub fn burn(&self) {
		if !self.0.b.fetch_or(true, SeqCst) {
			self.0.n.notify_waiters();
		}
	}

	/// Checks whether the fuze is burnt or not.
	pub fn burnt(&self) -> bool {
		self.0.b.load(SeqCst)
	}

	/// Blocks (async-ly) until the fuze is burnt, then never blocks again.
	pub async fn wait(&self) {
		if !self.burnt() {
			self.0.n.notified().await;
			self.0.b.store(true, SeqCst);
		}
	}
}
