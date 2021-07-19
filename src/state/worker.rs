use std::{
	sync::{
		atomic::{AtomicUsize, Ordering::SeqCst},
		Arc,
	},
	time::Duration,
};

use crate::protocols::gearman::{Request, Response};
use async_std::{
	channel::{bounded, Sender},
	net::{SocketAddr, TcpStream},
	path::Path,
	task::{spawn, JoinHandle},
};
use color_eyre::eyre::Result;
use futures::{future::try_join_all, io::AsyncReadExt};
use fuze::Fuze;
use log::{debug, info};

#[derive(Debug)]
pub struct Worker {
	pub name: Arc<str>,
	pub executor: Arc<Path>,
	pub concurrency: usize,
	pub timeout: Duration,
	pub client_id: Arc<str>,
	pub current_load: AtomicUsize,
	pub exit: Fuze<()>,
}

impl Worker {
	pub async fn run(self: Arc<Self>, server: SocketAddr) -> Result<()> {
		debug!("[{}] connecting to gearman", self.name);
		let mut gear = TcpStream::connect(server).await?;

		debug!(
			"[{}] naming ourself client_id={:?}",
			self.name, &self.client_id
		);
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

		let exitter = self.clone().exitter(res_s.clone(), req_s.clone());
		let reader = self.clone().reader(gear_read, res_s.clone(), req_s.clone());
		let writer = self.clone().writer(gear_write, req_r);
		let assignee = self.clone().assignee(res_r, req_s.clone());

		if let Err(err) = try_join_all([exitter, reader, writer, assignee]).await {
			if self.exit.burnt() {
				info!("[{}] worker stopped", self.name);
			} else {
				debug!(
					"[{}] got fatal error, trying to run exitter anyway",
					self.name
				);
				self.exit.burn();
				self.clone().exitter(res_s, req_s).await?;
			}

			Err(err)
		} else {
			info!("[{}] worker stopped", self.name);
			Ok(())
		}
	}

	fn exitter(
		self: Arc<Self>,
		res_s: Sender<Response>,
		req_s: Sender<Request>,
	) -> JoinHandle<Result<()>> {
		spawn(async move {
			self.exit.wait().await;
			debug!("[{}] exitter task running", self.name);

			// closing the res channel will end the assignee task
			res_s.close();

			if self.current_load.load(SeqCst) > 0 {
				// we need to wait for the worker tasks to finish
				// before we can really exit
				todo!();
			} else {
				req_s.close();
			}

			Ok(())
		})
	}
}
