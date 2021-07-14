use std::{
	sync::{
		atomic::{AtomicBool, AtomicUsize},
		Arc,
	},
	time::Duration,
};

use async_std::{
	net::{SocketAddr, ToSocketAddrs},
	path::Path,
	task::spawn,
};
use color_eyre::eyre::{eyre, Result};
use dashmap::DashMap;
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
	workers: DashMap<Arc<str>, Arc<Worker>>,
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
		);

		info!("preparing superman");
		info!("gearman server = {}", server);
		info!("base id = {}", base_id);

		Ok(Arc::new(Self {
			server,
			base_id,
			workers: DashMap::new(),
		}))
	}

	pub fn start_worker(
		self: Arc<Self>,
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
			client_id: format!("{}::{}={}", self.base_id, name, concurrency),
			current_load: AtomicUsize::new(0),
			exit: AtomicBool::new(false),
		});

		self.workers.insert(name, wrk.clone());
		spawn(wrk.run(self.server));
	}
}
