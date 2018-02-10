use std::any::{Any, TypeId};
use std::rc::{Rc, Weak};
use self::funcs::{RemoteFunc};
use utils::uuid::{UUID, UNIT_UUID};
#[macro_use]
pub mod macros;
pub mod funcs;