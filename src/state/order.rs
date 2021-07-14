use std::{
	ffi::OsString,
	os::unix::ffi::OsStringExt,
	sync::{
		atomic::{AtomicBool, Ordering::SeqCst},
		Arc,
	},
	time::Duration,
};

use crate::packet::Request;
use async_std::{
	channel::Sender,
	io::{timeout, BufReader},
	path::Path,
	process::{ChildStdout, Command, Stdio},
	task::spawn,
};
use color_eyre::eyre::{eyre, Result};
use futures::{AsyncBufReadExt, AsyncWriteExt, StreamExt};
use log::{debug, error, trace, warn};

#[derive(Clone, Debug)]
pub struct Order {
	pub log_prefix: String,

	pub name: Arc<str>,
	pub executor: Arc<Path>,
	pub timeout: Duration,

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
			timeout: self.timeout,
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

		let mut cmd = Command::new(self.executor.as_ref())
			.kill_on_drop(true)
			.current_dir(self.executor.parent().unwrap_or_else(|| Path::new("/tmp")))
			.stdin(Stdio::piped())
			.stdout(Stdio::piped())
			.stderr(Stdio::inherit())
			.arg(self.name.as_ref())
			.arg(&self.unique)
			.arg(self.workload.len().to_string())
			.spawn()?;

		debug!("{} spawned order pid={}", self.log_prefix, cmd.id());

		let sent_complete = Arc::new(AtomicBool::new(false));

		let reader = spawn(
			self.clone().reader(
				req_s.clone(),
				sent_complete.clone(),
				cmd.stdout
					.take()
					.ok_or_else(|| eyre!("missing stdout for command"))?,
			),
		);

		trace!("{} installed stdout reader", self.log_prefix);

		{
			let mut stdin = cmd
				.stdin
				.take()
				.ok_or_else(|| eyre!("missing stdin for command"))?;
			trace!("{} writing workload to stdin", self.log_prefix);
			stdin.write_all(self.workload.as_slice()).await?;
			trace!("{} closing stdin", self.log_prefix);
			stdin.close().await?;
			debug!(
				"{} wrote workload bytes={} to stdin",
				self.log_prefix,
				self.workload.len()
			);
		}

		debug!(
			"{} waiting on process completion, or timeout={:?}",
			self.log_prefix, self.timeout
		);

		// Result is for the process behaviour (actual I/O errors, raise as superman errors),
		// Option is for the timeout (report as order exception).
		match timeout(self.timeout, cmd.status())
			.await
			.map(Some)
			.or_else(|err| {
				if let std::io::ErrorKind::TimedOut = err.kind() {
					Ok(None)
				} else {
					Err(err)
				}
			})? {
			Some(s) if s.success() => {
				debug!("{} order exited with success", self.log_prefix);
			}
			Some(s) => {
				warn!("{} order exited with code={:?}", self.log_prefix, s);

				req_s
					.send(self.exception(format!(
						r#"{{"error":"order process exited with code={:?}"}}"#,
						s
					)))
					.await?;
				sent_complete.store(true, SeqCst);
			}
			None => {
				warn!(
					"{} order timed out after duration={:?}, killed",
					self.log_prefix, self.timeout
				);

				req_s
					.send(self.exception(format!(
						r#"{{"error":"order timed out after duration={:?}"}}"#,
						self.timeout
					)))
					.await?;
				sent_complete.store(true, SeqCst);
			}
		};

		if sent_complete.load(SeqCst) {
			debug!("{} order done, complete already sent", self.log_prefix);
		} else {
			debug!("{} order done, sending empty complete", self.log_prefix);
			req_s.send(self.complete([])).await?;
		}

		reader.await?;

		Ok(())
	}

	async fn reader(
		self: Arc<Self>,
		_req_s: Sender<Request>,
		_sent_complete: Arc<AtomicBool>,
		stdout: ChildStdout,
	) -> Result<()> {
		let stdout = BufReader::new(stdout);
		let mut lines = stdout.lines();
		while let Some(line) = lines.next().await {
			trace!("{} line from stdout: {:?}", self.log_prefix, line?);
		}

		Ok(())
	}

	fn exception(&self, data: impl Into<Vec<u8>>) -> Request {
		Request::WorkException {
			handle: self.handle.clone(),
			data: data.into(),
		}
	}

	fn complete(&self, data: impl Into<Vec<u8>>) -> Request {
		Request::WorkComplete {
			handle: self.handle.clone(),
			data: data.into(),
		}
	}
}
