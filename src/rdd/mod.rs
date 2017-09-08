mod funcs;
mod transformations;
use self::funcs::RDDFunc;
use self::transformations::*;

pub trait RDD<IN> {
    fn map<FN, OUT>(func: FN) -> MapRDD<FN, IN, OUT> where FN: RDDFunc<IN, OUT> {
        unimplemented!()
    }
    fn filter<FN>(func: FN) -> FilterRDD<FN, IN> where FN: RDDFunc<IN, bool> {
        unimplemented!()
    }
    fn flat_map<FN, OUT>(func: FN) -> FlatMapRDD<FN, IN, OUT> where FN: RDDFunc<IN, OUT> {
        unimplemented!()
    }
}
