use futures::future::BoxFuture;

pub trait FromToBytes {
  fn from_bytes(bytes: &[u8]) -> Self;
  fn to_bytes(&self) -> Vec<u8>;
}

pub enum FuncBody<'a, T: FromToBytes> {
  Asynchronous(fn(T) -> BoxFuture<'a, T>),
  Synchronous(fn(T) -> T),
  Stateful(fn(u32, T) -> BoxFuture<'a, T>), // PID, Parameters
}


pub struct Func<'a, T: FromToBytes> {
  pub body: FuncBody<'a, T>,
  pub func_id: u64
}