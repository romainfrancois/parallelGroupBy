benchmark_make_index <- function(data, by, ...){
  microbenchmark( 
    make_index_impl1_serial(data, by), 
    make_index_parallel(data,by), 
    make_index_concurrent_hash_map(data,by),
    make_index_threads_unorderedmap_joinConcurrentMap(data,by), 
    ...
  )
}

