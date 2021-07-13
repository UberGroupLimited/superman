use std::{ffi::OsString, os::unix::ffi::OsStringExt, sync::Arc, time::Duration};

use crate::packet::Request;
use async_std::{
	channel::Sender,
	io::{prelude::*, BufReader},
	path::{Path, PathBuf},
	process::{ChildStdout, Command, Stdio},
	task::{sleep, spawn},
};
use color_eyre::eyre::{eyre, Result};
use futures::StreamExt;
use log::{debug, error, trace};

#[derive(Clone, Debug)]
pub struct Order {
	pub log_prefix: String,
	pub name: String,
	pub executor: PathBuf,
	pub handle: Vec<u8>,
	pub unique: OsString,
	pub workload: Vec<u8>,
}

impl super::Worker {
	pub fn order(&self, handle: Vec<u8>, unique: Vec<u8>, workload: Vec<u8>) -> Arc<Order> {
		let log_prefix = format!("[{}] [{}]", self.name, hex::encode(&handle));

		let unique = OsString::from_vec(unique);

		debug!(
			"{} making order for job unique={:?} workload bytes={}",
			&log_prefix,
			&unique,
			workload.len()
		);

		Arc::new(Order {
			log_prefix,
			name: self.name.clone(),
			executor: self.executor.clone(),
			handle,
			unique,
			workload,
		})
	}
}

impl Order {
	pub async fn run(self: Arc<Self>, req_s: Sender<Request>) {
		if let Err(err) = self.clone().inner(req_s).await {
			error!(
				"{} running order errored (not the workload itself)",
				self.log_prefix
			);
			error!("{} {}", self.log_prefix, err);
		}
	}

	async fn inner(self: Arc<Self>, req_s: Sender<Request>) -> Result<()> {
		debug!("{} starting order", self.log_prefix);

		let mut cmd = Command::new(&self.executor)
			.kill_on_drop(true)
			.current_dir(self.executor.parent().unwrap_or(Path::new("/tmp")))
			.stdin(Stdio::piped())
			.stdout(Stdio::piped())
			.stderr(Stdio::inherit())
			.arg(&self.name)
			.arg(&self.unique)
			.arg(self.workload.len().to_string())
			.spawn()?;

		debug!("{} spawned order pid={}", self.log_prefix, cmd.id());

		let reader = spawn(
			self.clone().reader(
				req_s.clone(),
				cmd.stdout
					.take()
					.ok_or_else(|| eyre!("missing stdout for command"))?,
			),
		);

		trace!("{} installed stdout reader", self.log_prefix);

		debug!("{} order done, sending complete", self.log_prefix);

		req_s
			.send(Request::WorkComplete {
				handle: self.handle.clone(),
				data: br#"{"error":null,"data":null}"#.to_vec(),
			})
			.await?;

		reader.await?;

		Ok(())
	}

	async fn reader(self: Arc<Self>, _req_s: Sender<Request>, stdout: ChildStdout) -> Result<()> {
		let stdout = BufReader::new(stdout);
		let mut lines = stdout.lines();
		while let Some(line) = lines.next().await {
			trace!("{} line from stdout: {:?}", self.log_prefix, line?);
		}

		Ok(())
	}
}
