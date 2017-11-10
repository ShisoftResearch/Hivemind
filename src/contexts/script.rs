use std::collections::BTreeMap;
use rdd::script::RDDScript;

// only for context transport
#[derive(Serialize, Deserialize)]
pub struct ContextScript {
    pub dag: BTreeMap<u64, RDDScript>
}