benchmark_make_index <- function(data, by, ...){
  microbenchmark( 
    serial          = make_index_impl1_serial(data, by), 
    parallelReduce  = make_index_parallel(data,by), 
    concurrent      = make_index_concurrent_hash_map(data,by),
    threads         = make_index_threads_unorderedmap_joinConcurrentMap(data,by), 
    ...
  )
}

details_make_index <- function(data, by, ...){
  timings <- list( 
    serial = detail_make_index_impl1_serial(data, by)[[1]], 
    parallelReduce = detail_make_index_parallel(data, by)[[1]],
    concurrent = detail_make_index_concurrent_hash_map(data, by)[[1]],
    threads = detail_make_index_threads_unorderedmap_joinConcurrentMap( data, by)[[1]]
  )
  timings
}

