// use uuid::Uuid;

use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};

use async_std::{
    channel::bounded,
    io::{ReadExt, Write},
    net::{SocketAddr, TcpStream, ToSocketAddrs},
    path::Path,
    prelude::*,
    task::{spawn, JoinHandle},
};
use color_eyre::eyre::{eyre, Result};
use deku::{prelude::DekuError, DekuContainerRead, DekuContainerWrite};
use futures::io::AsyncReadExt;
use log::{debug, info, trace, warn};
use packet::{Packet, Request, Response};
use structopt::StructOpt;

mod packet;

#[derive(StructOpt, Debug)]
struct Args {
    #[structopt(short = "V", long, default_value = "info", env = "SUPERMAN_VERBOSE")]
    pub log_level: log::Level,

    #[structopt(
        short,
        long,
        default_value = "127.0.0.1:4730",
        env = "SUPERMAN_CONNECT"
    )]
    pub connect: String,
}

#[async_std::main]
async fn main() -> Result<()> {
    color_eyre::install()?;
    let args = Args::from_args_safe()?;

    stderrlog::new()
        .verbosity(match args.log_level {
            log::Level::Error => 0,
            log::Level::Warn => 1,
            log::Level::Info => 2,
            log::Level::Debug => 3,
            log::Level::Trace => 4,
        })
        .timestamp(stderrlog::Timestamp::Millisecond)
        .show_module_names(true)
        .module("superman")
        .init()?;

    let state = State::create(args.connect).await?;
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

        let (pkt_s, mut pkt_r) = bounded(512);

        let reader: JoinHandle<Result<()>> = spawn(async move {
            let mut packet = Vec::with_capacity(1024);
            'recv: loop {
                let mut buf = vec![0_u8; 1024];
                let len = ReadExt::read(&mut gear_read, &mut buf).await?;
                packet.extend(&buf[0..len]);
                trace!("received packet bytes: {:?}", &packet);

                'parse: loop {
                    if packet.is_empty() {
                        break 'parse;
                    }

                    match Packet::from_bytes((&packet, 0)) {
                        Ok(((rest, _), pkt)) => {
                            debug!("parsed packet: {:?}", &pkt);

                            if !rest.is_empty() {
                                trace!("data left: {} bytes", rest.len());
                                packet = rest.to_vec();
                            }

                            if let Some(res @ Response::JobAssignUniq { .. }) = pkt.response {
                                pkt_s.send(res).await?;
                            } else {
                                debug!("ignoring irrelevant packet");
                            }

                            continue 'parse;
                        }
                        Err(DekuError::Parse(msg)) => {
                            if msg.contains("not enough data") {
                                debug!("got partial packet, waiting for more");
                                continue 'recv;
                            } else {
                                warn!("bad packet, throwing away {} bytes", packet.len());
                                warn!("parsing error: {}", msg);
                                continue 'recv;
                            }
                        }
                        Err(err) => {
                            warn!("bad packet, throwing away {} bytes", packet.len());
                            warn!("parsing error: {}", err);
                            continue 'recv;
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
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        let hash = hasher.finish();

        debug!("request [{:x}] sending: {:?}", &hash, &self);
        let data = Packet::request(self)?.to_bytes()?;
        debug!(
            "request [{:x}] writing {} bytes to stream",
            &hash,
            data.len()
        );
        stream.write_all(&data).await?;
        debug!("request [{:x}] done writing to stream", &hash);

        Ok(())
    }
}
