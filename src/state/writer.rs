use std::{
	collections::hash_map::DefaultHasher,
	hash::{Hash, Hasher},
	sync::Arc,
};

use crate::protocols::gearman::{Packet, Request};
use async_std::{
	channel::Receiver,
	net::TcpStream,
	prelude::*,
	task::{spawn, JoinHandle},
};
use color_eyre::eyre::Result;
use deku::DekuContainerWrite;
use futures::{io::WriteHalf, AsyncWrite, StreamExt};
use log::{debug, trace};

impl super::Worker {
	pub fn writer(
		self: Arc<Self>,
		mut gear_write: WriteHalf<TcpStream>,
		mut req_r: Receiver<Request>,
	) -> JoinHandle<Result<()>> {
		spawn(async move {
			let mut sent_cant = false;

			while let Some(pkt) = req_r.next().await {
				if !sent_cant && self.exit.burnt() {
					Request::CantDo {
						name: self.name.as_bytes().to_vec(),
					}
					.send(&mut gear_write)
					.await?;
					sent_cant = true;
				}

				debug!("[{}] sending {} ({})", self.name, pkt.name(), pkt.id());
				pkt.send(&mut gear_write).await?;
			}

			debug!("[{}] writer task exiting", self.name);

			if !sent_cant {
				Request::CantDo {
					name: self.name.as_bytes().to_vec(),
				}
				.send(&mut gear_write)
				.await?;
			}

			Ok(())
		})
	}
}

impl Request {
	pub(crate) async fn send(self, stream: &mut (impl AsyncWrite + Unpin)) -> Result<()> {
		let mut hasher = DefaultHasher::new();
		self.hash(&mut hasher);
		let hash = hasher.finish();

		trace!("request [{:x}] sending: {:?}", &hash, &self);
		let data = Packet::request(self)?.to_bytes()?;
		trace!(
			"request [{:x}] writing {} bytes to stream",
			&hash,
			data.len()
		);
		stream.write_all(&data).await?;
		trace!("request [{:x}] done writing to stream", &hash);

		Ok(())
	}
}
