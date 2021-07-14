use std::{
	sync::{
		atomic::{AtomicBool, AtomicUsize},
		Arc,
	},
	time::Duration,
};

use crate::packet::Request;
use async_std::{
	channel::bounded,
	net::{SocketAddr, TcpStream},
	path::PathBuf,
};
use color_eyre::eyre::Result;
use futures::io::AsyncReadExt;
use log::debug;

#[derive(Debug)]
pub struct Worker {
	pub name: Arc<str>,
	pub executor: PathBuf,
	pub concurrency: usize,
	pub timeout: Duration,
	pub client_id: String,
	pub current_load: AtomicUsize,
	pub exit: AtomicBool,
}

impl Worker {
	pub async fn run(self: Arc<Self>, server: SocketAddr) -> Result<()> {
		debug!("[{}] connecting to gearman", self.name);
		let mut gear = TcpStream::connect(server).await?;

		debug!("naming ourself client_id={:?}", &self.client_id);
		let client_id = self.client_id.as_bytes().to_vec();
		Request::SetClientId { id: client_id }
			.send(&mut gear)
			.await?;

		debug!("[{}] declaring ourself for job", self.name);
		Request::CanDo {
			name: self.name.as_bytes().to_vec(),
		}
		.send(&mut gear)
		.await?;

		debug!("[{}] waiting for work", self.name);
		Request::PreSleep.send(&mut gear).await?;

		let (gear_read, gear_write) = gear.split();
		let (res_s, res_r) = bounded(512);
		let (req_s, req_r) = bounded(512);

		let reader = self.clone().reader(gear_read, res_s, req_s.clone());
		let writer = self.clone().writer(gear_write, req_r);
		let assignee = self.clone().assignee(res_r, req_s);

		// try join or something
		reader.await?;
		writer.await?;
		assignee.await?;

		Ok(())
	}
}
