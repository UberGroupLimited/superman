use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};

use crate::packet::{Packet, Request};
use async_std::{
    channel::bounded,
    io::Write,
    net::{SocketAddr, TcpStream, ToSocketAddrs},
    path::Path,
    prelude::*,
};
use color_eyre::eyre::{eyre, Result};
use deku::DekuContainerWrite;
use futures::io::AsyncReadExt;
use log::{debug, info, trace};

mod assignee;
mod reader;
mod writer;

#[derive(Debug)]
pub struct State {
    server: SocketAddr,
    base_id: String,
}

impl State {
    pub async fn create(server: impl ToSocketAddrs) -> Result<Self> {
        let server = server
            .to_socket_addrs()
            .await?
            .next()
            .ok_or(eyre!("no server addr provided"))?;

        let base_id = format!(
            "{}::v{}::{}",
            env!("CARGO_PKG_NAME"),
            env!("CARGO_PKG_VERSION"),
            hostname::get()?
                .into_string()
                .map_err(|s| eyre!("Hostname isn't UTF-8: {:?}", s))?
        );

        info!("preparing superman");
        info!("gearman server = {}", server);
        info!("base id = {}", base_id);

        Ok(Self { server, base_id })
    }
}

impl State {
    pub async fn worker(
        &self,
        name: &str,
        executor: impl AsRef<Path>,
        concurrency: usize,
    ) -> Result<()> {
        debug!("connecting to gearman");
        let mut gear = TcpStream::connect(self.server).await?;

        let client_id = format!("{}::{}={}", self.base_id, name, concurrency);
        debug!("naming ourself client_id={:?}", &client_id);
        let client_id = client_id.as_bytes().to_vec();
        Request::SetClientId { id: client_id }
            .send(&mut gear)
            .await?;

        debug!("declaring ourself for job={:?}", &name);
        Request::CanDo {
            name: name.as_bytes().to_vec(),
        }
        .send(&mut gear)
        .await?;

        debug!("waiting for work");
        Request::PreSleep.send(&mut gear).await?;

        let (gear_read, gear_write) = gear.split();

        let (res_s, res_r) = bounded(512);
        let (req_s, req_r) = bounded(512);

        let reader = reader::spawn(gear_read, res_s, req_s.clone());
        let writer = writer::spawn(gear_write, req_r);
        let assignee = assignee::spawn(res_r, req_s);

        // try join or something
        reader.await?;
        writer.await?;
        assignee.await?;

        Ok(())
    }
}

impl Request {
    pub(crate) async fn send(self, stream: &mut (impl Write + Unpin)) -> Result<()> {
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