use std::marker::PhantomData;

pub struct DataSet <T> {
    mark: PhantomData<T>
}