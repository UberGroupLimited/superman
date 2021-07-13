use std::{sync::Arc, time::Duration};

use crate::packet::Request;
use async_std::{channel::Sender, task::sleep};
use log::debug;

#[derive(Clone, Debug)]
pub struct Order {
	pub log_prefix: String,
	pub handle: Vec<u8>,
	pub unique: Vec<u8>,
	pub workload: Vec<u8>,
}

impl super::Worker {
	pub fn order(&self, handle: Vec<u8>, unique: Vec<u8>, workload: Vec<u8>) -> Arc<Order> {
		let log_prefix = format!("[{}] [{}]", self.name, hex::encode(&handle));

		debug!(
			"{} making order for job unique={:?} workload bytes={}",
			&log_prefix,
			std::str::from_utf8(&unique)
				.map(|s| s.to_owned())
				.unwrap_or_else(|_| hex::encode(&unique)),
			workload.len()
		);

		Arc::new(Order {
			log_prefix,
			handle,
			unique,
			workload,
		})
	}
}

impl Order {
	pub async fn run(self: Arc<Self>, req_s: Sender<Request>) {
		debug!("{} starting order", self.log_prefix);

		sleep(Duration::from_secs(4)).await;

		debug!("{} order done, sending complete", self.log_prefix);

		req_s
			.send(Request::WorkComplete {
				handle: self.handle.clone(),
				data: br#"{"error":null,"data":null}"#.to_vec(),
			})
			.await
			.expect("wrap with a try");
	}
}
