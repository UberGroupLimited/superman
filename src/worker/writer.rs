use crate::packet::Request;
use async_std::{
    channel::Receiver,
    net::TcpStream,
    task::{self, JoinHandle},
};
use color_eyre::eyre::Result;
use futures::{io::WriteHalf, StreamExt};

pub fn spawn(
    mut gear_write: WriteHalf<TcpStream>,
    mut req_r: Receiver<Request>,
) -> JoinHandle<Result<()>> {
    task::spawn(async move {
        while let Some(pkt) = req_r.next().await {
            dbg!(&pkt);
            pkt.send(&mut gear_write).await?;
        }

        Ok(())
    })
}
