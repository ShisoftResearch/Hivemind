use rdd::RDD;
use std::marker::PhantomData;

pub struct MapPartitationsRDD<FN, IN, OUT> {
    func_id: u64,
    marker: PhantomData<(FN, IN, OUT)>
}

impl<FN, IN, OUT> RDD<IN> for MapPartitationsRDD<FN, IN, OUT> {

}