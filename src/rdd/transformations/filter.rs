use rdd::RDD;
use std::marker::PhantomData;

pub struct FilterRDD<FN, IN> {
    func_id: u64,
    marker: PhantomData<(FN, IN)>
}

impl<FN, IN> RDD<IN> for FilterRDD<FN, IN> {

}