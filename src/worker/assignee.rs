use crate::packet::{Request, Response};
use async_std::{channel::{Receiver, Sender}, task::{self, JoinHandle}};
use color_eyre::eyre::{Result};
use futures::StreamExt;

pub fn spawn(
    mut res_r: Receiver<Response>,
    req_s: Sender<Request>,
) -> JoinHandle<Result<()>> {
    task::spawn(async move {
        while let Some(pkt) = res_r.next().await {
            dbg!(&pkt);
            if let Response::JobAssignUniq { handle, name, unique, workload } = pkt {
                req_s.send(Request::WorkComplete {
                    handle,
                    data: Vec::new(),
                }).await?;
            }
        }

        Ok(())
    })
}
