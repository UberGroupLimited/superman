use std::{
	ffi::OsString,
	os::unix::ffi::OsStringExt,
	sync::{
		atomic::{AtomicBool, Ordering::SeqCst},
		Arc,
	},
	time::Duration,
};

use crate::protocols::{gearman::Request, order};
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

#[derive(Debug)]
pub struct Order {
	pub log_prefix: String,

	pub name: Arc<str>,
	pub executor: Arc<Path>,
	pub timeout: Duration,

	pub handle: Vec<u8>,
	pub unique: OsString,
	pub workload: Vec<u8>,

	pub req_s: Sender<Request>,
	pub sent_complete: Arc<AtomicBool>,
}

impl super::Worker {
	pub fn order(
		&self,
		handle: Vec<u8>,
		unique: Vec<u8>,
		workload: Vec<u8>,
		req_s: Sender<Request>,
	) -> Arc<Order> {
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

			req_s,
			sent_complete: Arc::new(AtomicBool::new(false)),
		})
	}
}

impl Order {
	pub async fn run(self: Arc<Self>) {
		if let Err(err) = self.clone().inner().await {
			error!(
				"{} running order errored (not the workload itself)",
				self.log_prefix
			);
			error!("{} {:?}", self.log_prefix, err);
		}
	}

	async fn inner(self: Arc<Self>) -> Result<()> {
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

		let reader = spawn(
			self.clone().reader(
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

		match timeout(self.timeout, cmd.status()).await {
			Ok(s) if s.success() => {
				debug!("{} order exited with success", self.log_prefix);
			}
			Ok(s) => {
				warn!("{} order exited with code={:?}", self.log_prefix, s);

				self.exception(format!(
					r#"{{"error":"order process exited with code={:?}"}}"#,
					s
				))
				.await?;
			}
			Err(e) if matches!(e.kind(), std::io::ErrorKind::TimedOut) => {
				warn!(
					"{} order timed out after duration={:?}, killed",
					self.log_prefix, self.timeout
				);

				self.exception(format!(
					r#"{{"error":"order timed out after duration={:?}"}}"#,
					self.timeout
				))
				.await?;
			}
			Err(e) => {
				return Err(e.into());
			}
		};

		if self.sent_complete.load(SeqCst) {
			debug!("{} order done, complete already sent", self.log_prefix);
		} else {
			debug!("{} order done, sending empty complete", self.log_prefix);
			self.complete([]).await?;
		}

		reader.await?;

		Ok(())
	}

	async fn reader(self: Arc<Self>, stdout: ChildStdout) -> Result<()> {
		let stdout = BufReader::new(stdout);
		let mut lines = stdout.lines();
		while let Some(line) = lines.next().await {
			let line = line.map_err(|e| e.into());
			if let Err(err) = self.clone().read_line(line).await {
				error!("{} while handling stdout line: {:?}", self.log_prefix, err);
			}
		}

		Ok(())
	}

	async fn read_line(self: Arc<Self>, line: Result<String>) -> Result<()> {
		let line = line?;
		trace!("{} line from stdout: {}", self.log_prefix, line);

		let message = serde_json::from_str(&line)?;
		trace!("{} stdout message {:?}", self.log_prefix, message);

		match message {
			order::Message::Complete(order::Complete {
				data,
				error,
				mut others,
			}) => {
				debug!("{} stdout got completion", self.log_prefix);
				if !data.is_null() {
					others.insert("data".into(), data);
				}
				if !error.is_null() {
					others.insert("error".into(), error);
				}
				self.complete(serde_json::to_vec(&others)?).await?;
			}
			order::Message::Update(order::Update { data }) => {
				debug!("{} stdout got update", self.log_prefix);
				self.data(serde_json::to_vec(&data)?).await?;
			}
			order::Message::Progress(order::Progress {
				numerator,
				denominator,
			}) => {
				debug!("{} stdout got progress", self.log_prefix);
				self.status(numerator, denominator).await?;
			}
			order::Message::Print(order::Print { content }) => {
				debug!("{} stdout got print", self.log_prefix);
				eprintln!("{}", content);
			}
		}

		Ok(())
	}

	async fn exception(&self, data: impl Into<Vec<u8>>) -> Result<()> {
		if self.sent_complete.load(SeqCst) {
			return Ok(());
		}

		self.req_s
			.send(Request::WorkException {
				handle: self.handle.clone(),
				data: data.into(),
			})
			.await?;

		self.sent_complete.store(true, SeqCst);
		Ok(())
	}

	async fn complete(&self, data: impl Into<Vec<u8>>) -> Result<()> {
		if self.sent_complete.load(SeqCst) {
			return Ok(());
		}

		self.req_s
			.send(Request::WorkComplete {
				handle: self.handle.clone(),
				data: data.into(),
			})
			.await?;

		self.sent_complete.store(true, SeqCst);
		Ok(())
	}

	async fn data(&self, data: impl Into<Vec<u8>>) -> Result<()> {
		if self.sent_complete.load(SeqCst) {
			return Ok(());
		}

		self.req_s
			.send(Request::WorkData {
				handle: self.handle.clone(),
				data: data.into(),
			})
			.await?;

		Ok(())
	}

	async fn status(&self, numerator: isize, denominator: isize) -> Result<()> {
		if self.sent_complete.load(SeqCst) {
			return Ok(());
		}

		self.req_s
			.send(Request::WorkStatus {
				handle: self.handle.clone(),
				numerator: numerator.to_string().into_bytes(),
				denominator: denominator.to_string().into_bytes(),
			})
			.await?;

		Ok(())
	}
}
