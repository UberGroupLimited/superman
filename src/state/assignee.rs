use std::{
	sync::{atomic::Ordering, Arc},
};

use crate::packet::{Request, Response};
use async_std::{
	channel::{Receiver, Sender},
	task::{spawn, JoinHandle},
};
use color_eyre::eyre::Result;
use futures::StreamExt;
use log::{debug, error, warn};

impl super::Worker {
	pub fn assignee(
		self: Arc<Self>,
		mut res_r: Receiver<Response>,
		req_s: Sender<Request>,
	) -> JoinHandle<Result<()>> {
		spawn(async move {
			while let Some(pkt) = res_r.next().await {
				dbg!(&pkt);
				if let Response::JobAssignUniq {
					handle,
					name,
					unique,
					workload,
				} = pkt
				{
					match String::from_utf8(name) {
						Ok(name) => {
							if name.trim_end_matches('\0') != self.name {
								error!(
									"[{}] job delivered is for wrong function ({:?})",
									self.name, name
								);
								continue;
							}
						}
						Err(err) => {
							warn!(
								"[{}] job delivered has non-utf-8 function name, will not handle",
								self.name
							);
							warn!("{}", err);
							continue;
						}
					}

					let handle_hex = hex::encode(&handle);

					debug!(
						"[{}] [{}] handling job unique={:?} workload bytes={}",
						self.name,
						&handle_hex,
						std::str::from_utf8(&unique)
							.map(|s| s.to_owned())
							.unwrap_or_else(|_| hex::encode(&unique)),
						workload.len()
					);

					if self.current_load.fetch_add(1, Ordering::Relaxed) + 1 < self.concurrency {
						debug!("[{}] can still do more work, asking", self.name,);
						req_s.send(Request::PreSleep).await?;
					}

					self.clone().run_order(
						req_s.clone(),
						format!("[{}] [{}]", self.name, &handle_hex),
						handle,
						unique,
						workload,
					);
				} else {
					error!("[{}] assignee got unexpected packet: {:?}", self.name, pkt);
				}
			}

			Ok(())
		})
	}
}
