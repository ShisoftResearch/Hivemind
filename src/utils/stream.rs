use futures::{Stream, Async, Poll};

pub struct RepeatVec<T> where T: Clone + Copy {
    vec: Vec<T>,
    pos: usize
}

impl <T> RepeatVec<T> where T: Clone + Copy {
    pub fn new(vec: Vec<T>) -> RepeatVec<T> {
        RepeatVec {
            vec, pos: 0
        }
    }
}

impl <T> Stream for RepeatVec<T> where T: Clone + Copy {
    type Item = T;
    type Error = ();

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        let item = self.vec[self.pos % self.vec.len()];
        self.pos += 1;
        Ok(Async::Ready(Some(item)))
    }
}