use std::{
	fmt,
	sync::{
		atomic::{AtomicBool, Ordering::SeqCst},
		Arc,
	},
};

use async_std::channel::{bounded, Receiver, RecvError, SendError, Sender};

/// A mechanism to wait for a single signal which can be checked at any time.
///
/// A Fuze can be checked synchronously, and will show up as burnt only once it’s been burnt. It
/// can be awaited while it’s unburnt, and it can be Cloned and used without `mut` at any time.
///
/// Useful for exit conditions and as a one-off no-payload channel.
///
/// # Example
///
/// ```
/// use fuze::Fuze;
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
///			f3.burn().await.unwrap();
///
///			assert_eq!(f3.burnt(), true);
///
///			// this second burn does nothing:
///			f3.burn().await.unwrap();
///
///			assert_eq!(f3.burnt(), true);
///		});
///
///		// this call will block until the f3.burn() call above:
///		f2.wait().await.unwrap();
///		assert_eq!(f2.burnt(), true);
///
///		// now that the fuze is burnt, this call returns immediately:
///		f2.wait().await.unwrap();
/// });
///
///	assert_eq!(f1.burnt(), true);
/// ```
#[derive(Clone)]
pub struct Fuze {
	s: Sender<()>,
	r: Receiver<()>,
	b: Arc<AtomicBool>,
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
	pub fn new() -> Self {
		let (s, r) = bounded(1);
		Self {
			s,
			r,
			b: Arc::default(),
		}
	}

	/// Burns the fuze, unblocking everything that is waiting on it.
	pub async fn burn(&self) -> Result<(), SendError<()>> {
		if !self.b.fetch_or(true, SeqCst) {
			self.s.send(()).await?;
		}

		Ok(())
	}

	/// Checks whether the fuze is burnt or not.
	pub fn burnt(&self) -> bool {
		self.b.load(SeqCst)
	}

	/// Blocks (async-ly) until the fuze is burnt, then never blocks again.
	pub async fn wait(&self) -> Result<(), RecvError> {
		if !self.b.load(SeqCst) {
			self.r.recv().await?;
		}

		Ok(())
	}
}
