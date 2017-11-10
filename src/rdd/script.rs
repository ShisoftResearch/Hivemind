
// only for RDD transport
#[derive(Serialize, Deserialize)]
pub struct RDDScript {
    pub func_id: u64,
    pub deps: Vec<u64>,
    pub closure: Vec<u8>
}