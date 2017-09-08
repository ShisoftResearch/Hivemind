use rdd::RDD;
use std::marker::PhantomData;

pub struct MapRDD<FN, IN, OUT> {
    func_id: u64,
    marker: PhantomData<(FN, IN, OUT)>
}

impl<FN, IN, OUT> RDD<IN> for MapRDD<FN, IN, OUT> {

}