use self::funcs::RDDFunc;
use super::contexts::JobContext;
use scheduler::dag::partitioner::Partitioner;
use std::any::{Any, TypeId};
use uuid::Uuid;
#[macro_use]
pub mod macros;
pub mod funcs;
pub mod script;
pub mod transformers;
pub mod composer;

pub type AnyIter = Box<Iterator<Item = Box<Any + 'static>> + 'static>;

#[derive(
    Ord, PartialOrd, PartialEq, Eq, Hash,
    Copy, Clone,
    Serialize, Deserialize
)]
pub struct RDDID {
    bytes: [u8; 16],
}

pub static UNIT_RDDID: RDDID = RDDID { bytes: [0u8; 16] };

impl RDDID {
    pub fn rand() -> RDDID {
        let uuid = Uuid::new_v4();
        RDDID {
            bytes: *(uuid.as_bytes())
        }
    }
}

pub struct Partition {
    pub index: usize,
    pub server: u64,
}

pub trait RDD {
    fn compute(
        &self,
        iter: AnyIter,
        partition: &Partition,
    ) -> AnyIter;
    fn get_dependencies(&self) -> &Vec<&Box<RDD>>;
    fn get_partitioner(&self) -> &Box<Partitioner>;
    fn id(&self) -> RDDID;
}

pub trait RDDTracker: RDD + Sized {
    fn trans_id() -> u64;
    fn new(params: Box<Any>) -> Result<Box<RDD>, String>;
    fn construct_arg (data: &Vec<u8>) -> Box<Any>;
    fn register();
}

pub struct RDDC {
    raw_bytes: Option<Vec<u8>>
}

