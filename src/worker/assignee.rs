use crate::packet::{Request, Response};
use async_std::{
    channel::{Receiver, Sender},
    task::{self, JoinHandle},
};
use color_eyre::eyre::Result;
use futures::StreamExt;
use log::{debug, error};

pub fn spawn(mut res_r: Receiver<Response>, req_s: Sender<Request>) -> JoinHandle<Result<()>> {
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

                debug!("[{}] work done, sending complete", &handle_hex);
                req_s
                    .send(Request::WorkComplete {
                        handle,
                        data: br#"{"error":null,"data":null}"#.to_vec(),
                    })
                    .await?;
            } else {
                error!("assignee got unexpected packet: {:?}", pkt);
            }
        }

        Ok(())
    })
}
