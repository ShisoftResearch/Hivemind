use uuid::Uuid;
use std::fmt;
use std::slice;
use byteorder::LittleEndian;
use byteorder::ByteOrder;

#[derive(
    Ord, PartialOrd, PartialEq, Eq, Hash,
    Copy, Clone,
    Serialize, Deserialize
)]
pub struct UUID {
    bytes: [u8; 16],
}

pub static UNIT_UUID: UUID = UUID { bytes: [0u8; 16] };

impl UUID {
    pub fn rand() -> UUID {
        let uuid = Uuid::new_v4();
        UUID {
            bytes: *(uuid.as_bytes())
        }
    }
    pub fn unit() -> UUID {
        UNIT_UUID
    }

    pub fn new(higher: u64, lower: u64) -> UUID {
        let mut hi = [0; 8];
        let mut lo = [0; 8];
        LittleEndian::write_u64(&mut hi, higher);
        LittleEndian::write_u64(&mut lo, lower);
        let mut res_vec = Vec::with_capacity(16);
        res_vec.extend_from_slice(&hi);
        res_vec.extend_from_slice(&lo);
        let mut res_arr = [0; 16];
        for i in 0..16 {
            res_arr[i] = res_vec[i];
        }
        UUID {
            bytes: res_arr
        }
    }
}

impl fmt::Display for UUID {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:X}{:X}{:X}{:X}{:X}{:X}{:X}{:X}{:X}{:X}{:X}{:X}{:X}{:X}{:X}{:X}",
        self.bytes[0],
        self.bytes[1],
        self.bytes[2],
        self.bytes[3],
        self.bytes[4],
        self.bytes[5],
        self.bytes[6],
        self.bytes[7],
        self.bytes[8],
        self.bytes[9],
        self.bytes[10],
        self.bytes[11],
        self.bytes[12],
        self.bytes[13],
        self.bytes[14],
        self.bytes[15]
        )
    }
}