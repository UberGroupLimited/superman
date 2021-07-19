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
///
/// With an existing channel:
///
/// ```
/// use fuze::Fuze;
/// use async_std::{channel, task};
/// use std::time::Duration;
///
/// let (s, r) = channel::unbounded::<bool>();
/// let f1 = Fuze::with_channel(s, r);
/// let f2 = f1.clone();
/// task::block_on(async move {
///		assert_eq!(f2.burnt(), false);
///
///		let f3 = f2.clone();
///		task::spawn(async move {
///			assert_eq!(f3.burnt(), false);
///			task::sleep(Duration::from_secs(1)).await;
///			f3.burn();
///			assert_eq!(f3.burnt(), true);
///		});
///
///		f2.wait().await;
///		assert_eq!(f2.burnt(), true);
/// });
///
///	assert_eq!(f1.burnt(), true);
/// ```
#[derive(Clone)]
pub struct Fuze<T> {
	s: Sender<T>,
	r: Receiver<T>,
}

impl<T> fmt::Debug for Fuze<T> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("Fuze")
			.field("burnt", &self.burnt())
			.finish()
	}
}

impl<T> Default for Fuze<T> {
	/// Creates a new unburnt fuze.
	///
	/// # Examples
	///
	/// Basic usage:
	///
	/// ```
	/// # use fuze::Fuze;
	/// let fuze = Fuze::<bool>::default();
	///	assert_eq!(fuze.burnt(), false);
	/// fuze.burn();
	///	assert_eq!(fuze.burnt(), true);
	/// ```
	///
	/// For `Fuze<()>` you should probably use `Fuze::new()` instead.
	///
	/// ```
	/// # use fuze::Fuze;
	/// let fuze = Fuze::new();
	///	assert_eq!(fuze.burnt(), false);
	/// fuze.burn();
	///	assert_eq!(fuze.burnt(), true);
	/// ```
	fn default() -> Self {
		let (s, r) = bounded(1);
		Self {
			s,
			r,
		}
	}
}

impl Fuze<()> {
	/// Creates a new unburnt fuze.
	pub fn new() -> Self {
		let (s, r) = bounded(1);
		Self {
			s,
			r,
		}
	}
}

impl<T> Fuze<T> {
	/// Creates a fuze from an existing channel.
	///
	/// Note that the fuze can then be burnt if the channel is closed outside
	/// of the fuze’s purview.
	pub fn with_channel(s: Sender<T>, r: Receiver<T>) -> Self {
		Self { s, r }
	}

	/// Burns the fuze, unblocking everything that is waiting on it.
	pub fn burn(&self)  {
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
