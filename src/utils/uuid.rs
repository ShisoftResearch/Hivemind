use uuid::Uuid;
use std::fmt;

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