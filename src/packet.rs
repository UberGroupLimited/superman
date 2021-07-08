use deku::{bitvec::BitVec, prelude::*};
use log::trace;

#[derive(Clone, Debug, Eq, PartialEq, DekuRead, DekuWrite)]
pub struct Packet {
    #[deku(
        update = "match (self.request.is_some(), self.response.is_some()) { (true, false) => PacketMagic::Request, (false, true) => PacketMagic::Response, _ => unreachable!(\"EITHER request or response must be provided\") }"
    )]
    magic: PacketMagic,
    #[deku(
        endian = "big",
        update = "self.request.as_ref().map(|r| r.id()).or_else(|| self.response.as_ref().map(|r| r.id())).expect(\"either request or response must be provided\")"
    )]
    kind: u32,
    #[deku(
        bytes = 4,
        endian = "big",
        update = "self.request.as_ref().map(|r| r.bytes()).or_else(|| self.response.as_ref().map(|r| r.bytes())).expect(\"either request or response must be provided\")"
    )]
    length: usize,
    #[deku(cond = "*magic == PacketMagic::Request", ctx = "*length, *kind")]
    pub request: Option<Request>,
    #[deku(cond = "*magic == PacketMagic::Response", ctx = "*length, *kind")]
    pub response: Option<Response>,
}

impl Packet {
    pub fn request(r: Request) -> Result<Self, DekuError> {
        trace!("converting request to packet: {:?}", r);
        let mut pkt = Self {
            magic: PacketMagic::Request,
            kind: 0,
            length: 0,
            request: Some(r),
            response: None,
        };
        pkt.update()?;
        Ok(pkt)
    }

    pub fn response(r: Response) -> Result<Self, DekuError> {
        trace!("converting response to packet: {:?}", r);
        let mut pkt = Self {
            magic: PacketMagic::Response,
            kind: 0,
            length: 0,
            request: None,
            response: Some(r),
        };
        pkt.update()?;
        Ok(pkt)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, DekuRead, DekuWrite)]
#[deku(type = "u32", endian = "big")]
enum PacketMagic {
    #[deku(id = "5391697")] // \0REQ
    Request,
    #[deku(id = "5391699")] // \0RES
    Response,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, DekuRead, DekuWrite)]
#[deku(ctx = "datalen: usize, kind: u32", id = "kind")]
pub enum Request {
    #[deku(id = "22")]
    SetClientId {
        #[deku(count = "datalen")]
        id: Vec<u8>,
    },
    #[deku(id = "1")]
    CanDo {
        #[deku(count = "datalen")]
        name: Vec<u8>,
    },
    #[deku(id = "2")]
    CantDo {
        #[deku(count = "datalen")]
        name: Vec<u8>,
    },
    #[deku(id = "4")]
    PreSleep,
    #[deku(id = "30")]
    GrabJobUniq,
    #[deku(id = "12")]
    WorkStatus {
        #[deku(until = "|v: &u8| *v == 0")]
        handle: Vec<u8>,
        #[deku(until = "|v: &u8| *v == 0")]
        numerator: Vec<u8>,
        #[deku(count = "datalen - (handle.len() + numerator.len())")]
        denominator: Vec<u8>,
    },
    #[deku(id = "13")]
    WorkComplete {
        #[deku(until = "|v: &u8| *v == 0")]
        handle: Vec<u8>,
        #[deku(count = "datalen - handle.len()")]
        data: Vec<u8>,
    },
    #[deku(id = "14")]
    WorkFail {
        #[deku(count = "datalen")]
        handle: Vec<u8>,
    },
    #[deku(id = "25")]
    WorkException {
        #[deku(until = "|v: &u8| *v == 0")]
        handle: Vec<u8>,
        #[deku(count = "datalen - handle.len()")]
        data: Vec<u8>,
    },
    #[deku(id = "28")]
    WorkData {
        #[deku(until = "|v: &u8| *v == 0")]
        handle: Vec<u8>,
        #[deku(count = "datalen - handle.len()")]
        data: Vec<u8>,
    },
}

impl Request {
    pub(crate) fn id(&self) -> u32 {
        match self {
            Self::SetClientId { .. } => 22,
            Self::CanDo { .. } => 1,
            Self::CantDo { .. } => 2,
            Self::PreSleep => 4,
            Self::GrabJobUniq => 30,
            Self::WorkStatus { .. } => 12,
            Self::WorkComplete { .. } => 13,
            Self::WorkFail { .. } => 14,
            Self::WorkException { .. } => 25,
            Self::WorkData { .. } => 28,
        }
    }

    pub(crate) fn bytes(&self) -> usize {
        let mut buf = BitVec::new();
        self.write(&mut buf, (0, 0)).unwrap();
        buf.len() / 8
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, DekuRead, DekuWrite)]
#[deku(ctx = "datalen: usize, kind: u32", id = "kind")]
pub enum Response {
    #[deku(id = "6")]
    Noop,
    #[deku(id = "10")]
    NoJob,
    #[deku(id = "31")]
    JobAssignUniq {
        #[deku(until = "|v: &u8| *v == 0")]
        handle: Vec<u8>,
        #[deku(until = "|v: &u8| *v == 0")]
        name: Vec<u8>,
        #[deku(until = "|v: &u8| *v == 0")]
        unique: Vec<u8>,
        #[deku(count = "datalen - (handle.len() + name.len() + unique.len())")]
        workload: Vec<u8>,
    },
}

impl Response {
    pub(crate) fn id(&self) -> u32 {
        match self {
            Self::Noop => 6,
            Self::NoJob => 10,
            Self::JobAssignUniq { .. } => 31,
        }
    }

    pub(crate) fn bytes(&self) -> usize {
        let mut buf = BitVec::new();
        self.write(&mut buf, (0, 0)).unwrap();
        buf.len() / 8
    }
}

#[cfg(test)]
mod tests {
    use super::{Packet, PacketMagic, Request, Response};
    use deku::prelude::*;
    use std::ffi::CString;

    fn get_bytes<T: DekuContainerWrite>(r: Result<T, DekuError>) -> Vec<u8> {
        r.unwrap().to_bytes().unwrap()
    }

    const MAGIC_REQ: u32 = u32::from_be_bytes(*b"\0REQ");
    const MAGIC_RES: u32 = u32::from_be_bytes(*b"\0RES");

    fn response_noop() -> Vec<u8> {
        let mut data: Vec<u8> = Vec::new();
        data.extend(&MAGIC_RES.to_be_bytes());
        data.extend(&6_u32.to_be_bytes());
        data.extend(&0_u32.to_be_bytes());
        data
    }

    #[test]
    fn read_response_noop() {
        let data = response_noop();
        let ((rest, _), pkt) = Packet::from_bytes((&data, 0)).unwrap();
        assert_eq!(rest, &[]);
        assert_eq!(pkt.magic, PacketMagic::Response);
        assert_eq!(pkt.response, Some(Response::Noop));
    }

    #[test]
    fn write_response_noop() {
        assert_eq!(get_bytes(Packet::response(Response::Noop)), response_noop());
    }

    fn response_nojob() -> Vec<u8> {
        let mut data: Vec<u8> = Vec::new();
        data.extend(&MAGIC_RES.to_be_bytes());
        data.extend(&10_u32.to_be_bytes());
        data.extend(&0_u32.to_be_bytes());
        data
    }

    #[test]
    fn read_response_nojob() {
        let data = response_nojob();
        let ((rest, _), pkt) = Packet::from_bytes((&data, 0)).unwrap();
        assert_eq!(rest, &[]);
        assert_eq!(pkt.magic, PacketMagic::Response);
        assert_eq!(pkt.response, Some(Response::NoJob));
    }

    #[test]
    fn write_response_nojob() {
        assert_eq!(
            get_bytes(Packet::response(Response::NoJob)),
            response_nojob()
        );
    }

    fn response_jobassignuniq(handle: &str, name: &str, unique: &[u8], workload: &[u8]) -> Vec<u8> {
        let bhandle = CString::new(handle).unwrap();
        let bhandle = bhandle.as_bytes_with_nul();
        let bname = CString::new(name).unwrap();
        let bname = bname.as_bytes_with_nul();
        let bunique = CString::new(unique).unwrap();
        let bunique = bunique.as_bytes_with_nul();

        let mut data: Vec<u8> = Vec::new();
        data.extend(&MAGIC_RES.to_be_bytes());
        data.extend(&31_u32.to_be_bytes());
        data.extend(
            &((bhandle.len() + bname.len() + bunique.len() + workload.len()) as u32).to_be_bytes(),
        );
        data.extend(bhandle);
        data.extend(bname);
        data.extend(bunique);
        data.extend(workload);
        data
    }

    #[test]
    fn read_response_jobassignuniq() {
        let data = response_jobassignuniq(
            "H:localhost:1",
            "gandhy_matlack",
            b"e2cb1f42-1181-476e-960a-2c157ddab8ab",
            b"[1,2,3]",
        );
        let ((rest, _), pkt) = Packet::from_bytes((&data, 0)).unwrap();
        assert_eq!(rest, &[]);
        assert_eq!(pkt.magic, PacketMagic::Response);
        assert_eq!(
            pkt.response,
            Some(Response::JobAssignUniq {
                handle: b"H:localhost:1\0".to_vec(),
                name: b"gandhy_matlack\0".to_vec(),
                unique: b"e2cb1f42-1181-476e-960a-2c157ddab8ab\0".to_vec(),
                workload: b"[1,2,3]".to_vec(),
            })
        );
    }

    #[test]
    fn write_response_jobassignuniq() {
        assert_eq!(
            get_bytes(Packet::response(Response::JobAssignUniq {
                handle: b"H:localhost:2\0".to_vec(),
                name: b"lahn_ditch\0".to_vec(),
                unique: b"8fdff463-4e6f-4c6f-8e22-d3b5ea35f6fe\0".to_vec(),
                workload: b"[9,8,7]".to_vec(),
            })),
            response_jobassignuniq(
                "H:localhost:2",
                "lahn_ditch",
                b"8fdff463-4e6f-4c6f-8e22-d3b5ea35f6fe",
                b"[9,8,7]"
            )
        );
    }

    fn request_cando(name: &str) -> Vec<u8> {
        let bname = name.as_bytes();
        let mut data: Vec<u8> = Vec::new();
        data.extend(&MAGIC_REQ.to_be_bytes());
        data.extend(&1_u32.to_be_bytes());
        data.extend(&(bname.len() as u32).to_be_bytes());
        data.extend(bname);
        data
    }

    #[test]
    fn read_request_cando() {
        let data = request_cando("helloworld");
        let ((rest, _), pkt) = Packet::from_bytes((&data, 0)).unwrap();
        assert_eq!(rest, &[]);
        assert_eq!(pkt.magic, PacketMagic::Request);
        assert_eq!(
            pkt.request,
            Some(Request::CanDo {
                name: b"helloworld".to_vec(),
            })
        );
    }

    #[test]
    fn write_request_cando() {
        assert_eq!(
            get_bytes(Packet::request(Request::CanDo {
                name: b"bananasplit".to_vec()
            })),
            request_cando("bananasplit")
        );
    }
}
