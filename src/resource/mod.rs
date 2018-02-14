use std::marker::PhantomData;
use utils::uuid::UUID;

pub enum Source <T> {
    Local(Box<IntoIterator<Item = T, IntoIter = Iterator<Item = T>>>),
    Remote(UUID)
}

pub struct DataSet <T> {
    source: Source<T>
}