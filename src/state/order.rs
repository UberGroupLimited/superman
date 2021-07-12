use std::{
	sync::{atomic::Ordering, Arc},
	time::Duration,
};

use crate::packet::{Request};
use async_std::{
	channel::{Sender},
	task::{spawn, sleep},
};
use log::{debug};

impl super::Worker {
	pub fn run_order(
		self: Arc<Self>,
		req_s: Sender<Request>,
		log_prefix: String,
		handle: Vec<u8>,
		unique: Vec<u8>,
		workload: Vec<u8>,
	) {
		spawn(async move {
			sleep(Duration::from_secs(4)).await;

			debug!(
				"{} work done, sending complete",
				&log_prefix
			);
			req_s
				.send(Request::WorkComplete {
					handle,
					data: br#"{"error":null,"data":null}"#.to_vec(),
				})
				.await
				.expect("wrap with a try");

			if self.current_load.fetch_sub(1, Ordering::Relaxed) - 1 < self.concurrency
			{
				debug!("[{}] can do more work now, asking", self.name);
				req_s
					.send(Request::PreSleep)
					.await
					.expect("wrap with a try");
			}
		});
	}
}
