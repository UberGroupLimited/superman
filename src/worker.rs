use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use crate::packet::Request;
use async_std::{
    channel::bounded,
    net::{SocketAddr, TcpStream, ToSocketAddrs},
    path::Path,
};
use color_eyre::eyre::{eyre, Result};
use futures::io::AsyncReadExt;
use log::{debug, info};

mod assignee;
mod reader;
mod writer;

#[derive(Debug)]
pub struct State {
    server: SocketAddr,
    base_id: String,
    workload: AtomicUsize,
    concurrency: AtomicUsize,
}

impl State {
    pub async fn create(server: impl ToSocketAddrs) -> Result<Arc<Self>> {
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

        Ok(Arc::new(Self {
            server,
            base_id,
            workload: AtomicUsize::new(0),
            concurrency: AtomicUsize::new(0),
        }))
    }
}

impl State {
    pub async fn worker(
        self: Arc<Self>,
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

        debug!("registering into state");
        self.concurrency.store(concurrency, Ordering::Relaxed);

        debug!("waiting for work");
        Request::PreSleep.send(&mut gear).await?;

        let (gear_read, gear_write) = gear.split();
        let (res_s, res_r) = bounded(512);
        let (req_s, req_r) = bounded(512);

        let reader = self.clone().reader(gear_read, res_s, req_s.clone());
        let writer = self.clone().writer(gear_write, req_r);
        let assignee = self.clone().assignee(res_r, req_s);

        // try join or something
        reader.await?;
        writer.await?;
        assignee.await?;

        Ok(())
    }
}
