use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

use crate::packet::{Request, Response};
use async_std::{
    channel::{Receiver, Sender},
    task::{self, sleep, JoinHandle},
};
use color_eyre::eyre::Result;
use futures::StreamExt;
use log::{debug, error};

impl super::Worker {
    pub fn assignee(
        self: Arc<Self>,
        mut res_r: Receiver<Response>,
        req_s: Sender<Request>,
    ) -> JoinHandle<Result<()>> {
        task::spawn(async move {
            while let Some(pkt) = res_r.next().await {
                dbg!(&pkt);
                if let Response::JobAssignUniq {
                    handle,
                    name,
                    unique,
                    workload,
                } = pkt
                {
                    let handle_hex = hex::encode(&handle);

                    debug!(
                        "[{}] handling job name={:?} unique={:?} workload bytes={}",
                        &handle_hex,
                        String::from_utf8_lossy(&name),
                        std::str::from_utf8(&unique)
                            .map(|s| s.to_owned())
                            .unwrap_or_else(|_| hex::encode(&unique)),
                        workload.len()
                    );

                    if self.current_load.fetch_add(1, Ordering::Relaxed) + 1 < self.concurrency {
                        debug!(
                            "[{}] can still do more work, asking",
                            String::from_utf8_lossy(&name),
                        );
                        req_s.send(Request::PreSleep).await?;
                    }

                    let this = self.clone();
                    let req_s = req_s.clone();
                    task::spawn(async move {
                        sleep(Duration::from_secs(4)).await;

                        debug!("[{}] work done, sending complete", &handle_hex);
                        req_s
                            .send(Request::WorkComplete {
                                handle,
                                data: br#"{"error":null,"data":null}"#.to_vec(),
                            })
                            .await
                            .expect("wrap with a try");

                        if this.current_load.fetch_sub(1, Ordering::Relaxed) - 1 < this.concurrency
                        {
                            debug!(
                                "[{}] can do more work now, asking",
                                String::from_utf8_lossy(&name),
                            );
                            req_s
                                .send(Request::PreSleep)
                                .await
                                .expect("wrap with a try");
                        }
                    });
                } else {
                    error!("assignee got unexpected packet: {:?}", pkt);
                }
            }

            Ok(())
        })
    }
}
