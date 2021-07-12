use std::sync::{
	atomic::{AtomicBool, AtomicUsize},
	Arc,
};

use async_std::{
	net::{SocketAddr, ToSocketAddrs},
	path::Path,
};
use color_eyre::eyre::{eyre, Result};
use log::info;

pub(self) use worker::Worker;

mod assignee;
mod order;
mod reader;
mod worker;
mod writer;

#[derive(Debug)]
pub struct State {
	server: SocketAddr,
	base_id: String,
	workload: AtomicUsize,
	concurrency: AtomicUsize,
}

impl State {
	pub async fn create(server: impl ToSocketAddrs) -> Result<Arc<Self>> {
		let server = server
			.to_socket_addrs()
			.await?
			.next()
			.ok_or(eyre!("no server addr provided"))?;

		let base_id = format!(
			"{}::v{}::{}",
			env!("CARGO_PKG_NAME"),
			env!("CARGO_PKG_VERSION"),
			hostname::get()?
				.into_string()
				.map_err(|s| eyre!("Hostname isn't UTF-8: {:?}", s))?
		);

		info!("preparing superman");
		info!("gearman server = {}", server);
		info!("base id = {}", base_id);

		Ok(Arc::new(Self {
			server,
			base_id,
			workload: AtomicUsize::new(0),
			concurrency: AtomicUsize::new(0),
		}))
	}

	pub async fn start_worker(
		self: Arc<Self>,
		name: &str,
		executor: impl AsRef<Path>,
		concurrency: usize,
	) -> Result<()> {
		let wrk = Arc::new(Worker {
			name: name.into(),
			executor: executor.as_ref().into(),
			concurrency,
			client_id: format!("{}::{}={}", self.base_id, name, concurrency),

			current_load: AtomicUsize::new(0),
			exit: AtomicBool::new(false),
		});
		// TODO: insert into state

		// TODO: spawn instead
		wrk.run(self.server).await?;
		Ok(())
	}
}
