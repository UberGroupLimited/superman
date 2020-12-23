// use uuid::Uuid;

use async_std::{
    io::{ReadExt, Write},
    net::{SocketAddr, TcpStream, ToSocketAddrs},
    path::Path,
    prelude::*,
    sync::channel,
    task::{spawn, JoinHandle},
};
use color_eyre::eyre::{eyre, Result};
use deku::{prelude::DekuError, DekuContainerRead, DekuContainerWrite};
use futures::io::AsyncReadExt;
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

        let mut gear = TcpStream::connect(self.server).await?;
        Request::SetClientId { id: client_id }
            .send(&mut gear)
            .await?;

        Request::CanDo {
            name: name.as_bytes().to_vec(),
        }
        .send(&mut gear)
        .await?;

        Request::PreSleep.send(&mut gear).await?;

        let (mut gear_read, gear_write) = gear.split();

        let (pkt_s, mut pkt_r) = channel(512);

        let reader: JoinHandle<Result<()>> = spawn(async move {
            let mut packet = Vec::with_capacity(1024);
            'recv: loop {
                let mut buf = vec![0_u8; 1024];
                let len = ReadExt::read(&mut gear_read, &mut buf).await?;
                packet.extend(&buf[0..len]);
                // todo: trace log(packet)

                'parse: loop {
                    if packet.is_empty() {
                        break 'parse;
                    }

                    match Packet::from_bytes((&packet, 0)) {
                        Ok(((rest, _), pkt)) => {
                            // todo: debug log(pkt)

                            if !rest.is_empty() {
                                // todo: trace log("parsing more")
                                packet = rest.to_vec();
                            }

                            if let Some(res @ Response::JobAssignUniq { .. }) = pkt.response {
                                pkt_s.send(res).await;
                            } else {
                                // ignore packet
                                // todo: debug log
                            }

                            continue 'parse;
                        }
                        Err(DekuError::Parse(msg)) => {
                            if msg.contains("not enough data") {
                                // todo: debug log
                                continue 'recv;
                            } else {
                                // todo: be tolerant of gearman errors
                                // warn and reset the buffer
                                Err(DekuError::Parse(msg))?;
                            }
                        }
                        Err(err) => {
                            // todo: be tolerant of gearman errors
                            // warn and reset the buffer
                            Err(err)?;
                        }
                    }
                }
            }
        });

        let assignee: JoinHandle<Result<()>> = spawn(async move {
            while let Some(pkt) = pkt_r.next().await {
                dbg!(&pkt);
            }

            Ok(())
        });

        reader.await?;
        assignee.await?;

        Ok(())
    }
}

impl Request {
    pub(crate) async fn send(self, stream: &mut (impl Write + Unpin)) -> Result<()> {
        let data = Packet::request(self)?.to_bytes()?;
        stream.write_all(&data).await?;
        Ok(())
    }
}
