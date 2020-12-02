// use uuid::Uuid;

use async_std::{
    net::{SocketAddr, TcpStream, ToSocketAddrs},
    path::Path,
    prelude::*,
};
use color_eyre::eyre::{eyre, Result};
use deku::DekuContainerWrite;
use packet::{Packet, Request, Response};

mod packet;

#[async_std::main]
async fn main() -> Result<()> {
    color_eyre::install()?;

    let state = State::create("127.0.0.1:4730").await?;
    state.worker("supertest", "/usr/bin/true", 1).await?;

    Ok(())
}

#[derive(Debug)]
struct State {
    server: SocketAddr,
    base_id: String,
}

impl State {
    async fn create(server: impl ToSocketAddrs) -> Result<Self> {
        Ok(Self {
            server: server
                .to_socket_addrs()
                .await?
                .next()
                .ok_or(eyre!("no server addr provided"))?,
            base_id: format!(
                "{}::v{}::{}",
                env!("CARGO_PKG_NAME"),
                env!("CARGO_PKG_VERSION"),
                hostname::get()?
                    .into_string()
                    .map_err(|s| eyre!("Hostname isn't UTF-8: {:?}", s))?
            ),
        })
    }

    async fn worker(
        &self,
        name: &str,
        executor: impl AsRef<Path>,
        concurrency: usize,
    ) -> Result<()> {
        let client_id = format!("{}::{}={}", self.base_id, name, concurrency)
            .as_bytes()
            .to_vec();

        let mut stream = TcpStream::connect(self.server).await?;
        Packet::request(Request::SetClientId { id: client_id })?
            .send(&mut stream)
            .await?;

        Packet::request(Request::CanDo {
            name: name.as_bytes().to_vec(),
        })?
        .send(&mut stream)
        .await?;

        Ok(())
    }
}

impl Packet {
    pub(crate) async fn send(self, stream: &mut TcpStream) -> Result<()> {
        let data = self.to_bytes()?;
        stream.write_all(&data).await?;
        Ok(())
    }
}
