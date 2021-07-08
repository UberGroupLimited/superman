use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    sync::Arc,
};

use crate::packet::{Packet, Request};
use async_std::{
    channel::Receiver,
    net::TcpStream,
    prelude::*,
    task::{self, JoinHandle},
};
use color_eyre::eyre::Result;
use deku::DekuContainerWrite;
use futures::{io::WriteHalf, AsyncWrite, StreamExt};
use log::trace;

impl super::State {
    pub fn writer(
        self: Arc<Self>,
        mut gear_write: WriteHalf<TcpStream>,
        mut req_r: Receiver<Request>,
    ) -> JoinHandle<Result<()>> {
        task::spawn(async move {
            while let Some(pkt) = req_r.next().await {
                pkt.send(&mut gear_write).await?;
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
