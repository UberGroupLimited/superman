use std::{
	sync::{atomic::AtomicUsize, Arc},
	time::Duration,
};

use async_std::{
	net::{SocketAddr, ToSocketAddrs},
	path::Path,
	task::spawn,
};
use color_eyre::eyre::{eyre, Result};
use dashmap::DashMap;
use fuze::Fuze;
use log::{info, trace};

pub(self) use worker::Worker;

mod assignee;
mod order;
mod reader;
mod worker;
mod writer;

#[derive(Debug)]
pub struct State {
	server: SocketAddr,
	base_id: Box<str>,
	workers: DashMap<Arc<str>, Arc<Worker>>,
	running: Fuze,
}

impl State {
	pub async fn create(server: impl ToSocketAddrs) -> Result<Arc<Self>> {
		let server = server
			.to_socket_addrs()
			.await?
			.next()
			.ok_or_else(|| eyre!("no server addr provided"))?;

		let base_id = format!(
			"{}::v{}::{}",
			env!("CARGO_PKG_NAME"),
			env!("CARGO_PKG_VERSION"),
			hostname::get()?
				.into_string()
				.map_err(|s| eyre!("Hostname isn't UTF-8: {:?}", s))?
		)
		.into_boxed_str();

		info!("preparing superman");
		info!("gearman server = {}", server);
		info!("base id = {}", base_id);

		Ok(Arc::new(Self {
			server,
			base_id,
			workers: DashMap::new(),
			running: Fuze::new(),
		}))
	}

	pub async fn wait(self: Arc<Self>) -> Result<()> {
		self.running.wait().await?;
		trace!("not running anymore");
		Ok(())
	}

	pub fn start_worker(
		&self,
		name: &str,
		executor: impl AsRef<Path>,
		concurrency: usize,
		timeout: Duration,
	) {
		let name = Arc::from(name.to_owned().into_boxed_str());
		let wrk = Arc::new(Worker {
			name: Arc::clone(&name),
			executor: executor.as_ref().into(),
			concurrency,
			timeout,
			client_id: format!("{}::{}={}", self.base_id, name, concurrency)
				.into_boxed_str()
				.into(),
			current_load: AtomicUsize::new(0),
			exit: Fuze::new(),
		});

		info!("[{}] creating worker instance", name);
		self.workers.insert(name, wrk.clone());
		spawn(wrk.run(self.server));
	}

	pub async fn stop_worker(&self, name: &str) -> Result<()> {
		if let Some(wrk) = self.workers.get(name) {
			wrk.exit.burn().await?;
			// worker will clean up after itself
			// TODO: ^ do that
		}

		Ok(())
	}
}
