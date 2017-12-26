// Range partitioner will do reservoir sampling on RDD to bring balance to each partition
// For sampling, the original RDDs should be expanded with sampling RDDs.
// Range partitioner must accept sampling RDDID that can extract upper bound, lower bound, and distributions

// Note 1: Spark utilized closure delivering feature from Scala and Java world that can transport
//  RDDs from clients directly to remotes, but in Hivemind this is not possible. Hivemind use a
//  a composer for easy of use LINQ-like expressions and compile it to serializable data structures
//  and send them to job scheduler.
//  In other word, for those operations in client side that require range partitioner, like
//  `order_by`, all require the composer to generate the required RDDs for sampling and sampling RDDs
//  need to be executed when partitioning at job scheduler when the job is running (composer does not
//  execute job of any kind).
//  The RDD expansion for sampling is static so it is safe to do it at client composer.
//  Partitioner will call `collected` at job scheduler to get the actual sample data from sampling RDD.

// Note 2: Range partitioner is meant to be uncommon for it's higher price of sampling. It should
//  only be used when necessary, like ordering. For `group_by` that may be unbalanced, we should
//  let it happened by using `HashPartitioner` instead

use contexts::script::RDDComposer;

pub fn plan_sampling<R, S>(rdd: &R)  -> S
    where S: RDDComposer,
          R: RDDComposer
{
    unimplemented!()
}

