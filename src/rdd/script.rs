
// only for RDD transport
pub struct RDDScript {
    deps: Vec<u64>,
    func_id: u64,
    closure: Vec<u8>
}