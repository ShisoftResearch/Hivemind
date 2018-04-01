use storage::block::BlockCursor;
use futures::prelude::*;

pub type BuffFut<T> = Box<Future<Item = (Vec<T>, BlockCursor), Error = String>>;

pub trait BufferedStreamableStorage<T> {
    #[inline]
    fn read_batch(&mut self) -> Box<Future<Item = (Vec<Vec<u8>>, BlockCursor), Error = String>>;
    #[inline]
    fn cursor(&mut self) -> &mut BlockCursor;
    #[inline]
    fn buffer(&mut self) -> &mut Box<Iterator<Item = T>>;
    #[inline]
    fn fut(&mut self) -> &mut Option<BuffFut<T>>;
}

macro_rules! impl_stream {
    ($struc: ident) => {
        impl <T> Stream for $struc<T> where T: DeserializeOwned + 'static {

            type Item = T;
            type Error = String;

            fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
                let iter_buf = self.buffer().next();
                if let Some(item) = iter_buf {
                    // item buffered and exists
                    return Ok(Async::Ready(Some(item)))
                } else {
                    // buffer is empty
                    let mut poll_res = None;
                    if let &mut Some(ref mut fut) = self.fut() {
                        // future exists, poll from it
                        match fut.poll() {
                            Ok(Async::Ready((items, cursor))) => {
                                if items.len() < 1 {
                                    return Ok(Async::Ready(None))
                                }
                                poll_res = Some((cursor.pos, box items.into_iter()));
                            },
                            Ok(Async::NotReady) => return Ok(Async::NotReady),
                            Err(e) => return Err(e)
                        }
                    }
                    if let Some((pos, iter)) = poll_res  {
                        self.cursor().pos = pos;
                        *self.buffer() = iter;
                        *self.fut() = None;
                        return self.poll()
                    }
                    // No future and iterator is empty, go get one
                    let new_fut = self.read_batch()
                        .map(|(ds, cursor)| {
                            (ds.iter().map(|d| bincode::deserialize(d)).collect(), cursor)
                        });
                    *self.fut() = Some(box new_fut);
                    return Ok(Async::NotReady)
                }
            }
        }
    };
}

macro_rules! impl_stream_sources {
    () => {
        // cursor: BlockCursor,
        // fut: Option<BuffFut<T>>,
        // buffer: Box<Iterator<Item = T>>

        #[inline]
        fn cursor(&mut self) -> &mut BlockCursor {
            &mut self.cursor
        }

        #[inline]
        fn buffer(&mut self) -> &mut Box<Iterator<Item=T>> {
            &mut self.buffer
        }

        #[inline]
        fn fut(&mut self) -> &mut Option<BuffFut<T>> {
            &mut self.fut
        }
    };
}