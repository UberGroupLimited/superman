use std::sync::{atomic::Ordering, Arc};

use crate::protocols::gearman::{Request, Response};
use async_std::{
	channel::{Receiver, Sender},
	task::{spawn, JoinHandle},
};
use color_eyre::eyre::Result;
use futures::{FutureExt, StreamExt};
use log::{debug, error, warn};

impl super::Worker {
	pub fn assignee(
		self: Arc<Self>,
		mut res_r: Receiver<Response>,
		req_s: Sender<Request>,
	) -> JoinHandle<Result<()>> {
		spawn(async move {
			while let Some(pkt) = res_r.next().await {
				if let Response::JobAssignUniq {
					handle,
					name,
					unique,
					workload,
				} = pkt
				{
					match String::from_utf8(name) {
						Ok(name) => {
							if name.trim_end_matches('\0') != self.name.as_ref() {
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

					if self.current_load.fetch_add(1, Ordering::Relaxed) + 1 < self.concurrency {
						debug!("[{}] can still do more work, asking", self.name,);
						req_s.send(Request::PreSleep).await?;
					}

					let order = self
						.clone()
						.order(handle, unique, workload, req_s.clone())
						.run();
					let this = self.clone();
					let req_ss = req_s.clone();
					spawn(order.then(|_| async move {
						if this.current_load.fetch_sub(1, Ordering::Relaxed) - 1 < this.concurrency
						{
							debug!("[{}] can do more work now, asking", this.name);
							req_ss
								.send(Request::PreSleep)
								.await
								.expect("wrap with a try");
						}
					}));
				} else {
					error!("[{}] assignee got unexpected packet: {:?}", self.name, pkt);
				}
			}

			debug!("[{}] assignee task exiting", self.name);
			Ok(())
		})
	}
}
