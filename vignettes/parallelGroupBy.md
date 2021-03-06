
```
## Loading required package: Rcpp
## Loading required package: RcppParallel
## Loading required package: BH
## Loading required package: microbenchmark
## Loading required package: dplyr
## 
## Attaching package: 'dplyr'
## 
## The following objects are masked from 'package:stats':
## 
##     filter, lag
## 
## The following objects are masked from 'package:base':
## 
##     intersect, setdiff, setequal, union
## 
## Loading required package: babynames
```

introduction
============

The `parallelGroupBy` package is an experiment to rewrite the core 
operations behind `dplyr::group_by` in parallel. For this experiment, 
some C++ code has been extracted from `dplyr`, mainly the part about 
hashing rows of a data frame with visitors. 

The main operation of `group_by` is training a hash map to collect all indices
from the same group as one index. The hash map is then transformed into 
a list of integer vectors, each of one being the 0-based indices for each group. 

`group_by` also handles other things such as labels, etc ... but for the purpose 
of this experiment, let's just focus on the core operation. 

serial implementation
=====================

The serial implementation will be our reference point. We will train 
a `boost::unordered_map<int, std::vector<int>, ., .>` from `BH` as this needs
to work on C++98. 

The implementation looks like this: 

```cpp
// [[Rcpp::export]]
List make_index_impl1_serial( DataFrame data, CharacterVector by ){
    int n = data.nrows() ;
    Visitors visitors(data, by) ;
    VisitorSetHasher<Visitors> hasher(visitors); 
    VisitorSetEqualPredicate<Visitors> equal(visitors);
    
    Map map(1024, hasher, equal);  
    for( int i=0; i<n; i++)
        map[i].push_back(i) ;
    
    int ngroups = map.size() ;
    List indices(ngroups) ;
    
    Map::const_iterator it = map.begin() ;
    for( int i=0; i<ngroups; i++, ++it){
        indices[i] = it->second ;
    }
    
    return indices ;
}
```

parallelReduce and independents hash maps
=========================================

The second implementation considered here leverages `parallelReduce`
from `RcppParallel`. The nice thing  about `parallelReduce` is that 
it isolates us from micro details about threads, etc ... 

We just have to make a worker class that follows a few guidelines: 

 - it needs an `operator()( size_t begin, size_t end)`. The scheduler splits
 the data as it sees fit and call this operator on sub ranges. 
 - it needs a `join()` method to join results from two workers
 
For this first parallel implementation, we will use the `IndexMaker`
class. Each instance of the class contain an independent hash map, which 
is trained in subsequent calls of the `operator()`, and `IndexMaker` are joined
with the `join` method. Finally, when there is only one left, i.e. when all
instances are joined, we can use the `get()` method which makes the 
R list. 

The function in itself is simple: 

```cpp
// [[Rcpp::export]]
List make_index_parallel( DataFrame data, CharacterVector by ){
    
    int n = data.nrows() ;
    
    Visitors visitors(data, by) ;
    IndexMaker indexer(visitors) ;
    
    parallelReduce(0, n, indexer) ;
    
    return indexer.get() ;
}
```

As all examples of `parallelReduce`, the interesting bits are in the 
implementation of the worker class, i.e. `IndexMaker`: 

```cpp
struct IndexMaker : public Worker {
    Visitors& visitors ;
    VisitorSetHasher<Visitors> hasher ; 
    VisitorSetEqualPredicate<Visitors> equal ;
    Map map ;
    
    IndexMaker( Visitors& visitors_ ) : 
        visitors(visitors_), 
        hasher(visitors), 
        equal(visitors), 
        map(1024, hasher, equal){}
    
    IndexMaker( const IndexMaker& other, Split) : 
        visitors(other.visitors), 
        hasher(visitors), 
        equal(visitors), 
        map(1024, hasher, equal){}
        
    void operator()(std::size_t begin, std::size_t end) {
        for( size_t i =begin; i<end; i++) map[i].push_back(i) ;    
    }
    
    void join(const IndexMaker& rhs) { 
        // join data from rhs into this. 
        Map::const_iterator it = rhs.map.begin() ;
        for( ; it != rhs.map.end(); ++it ){
            // find if it exist in this map
            std::vector<int>& v = map[it->first] ;
            
            v.insert( v.end(), it->second.begin(), it->second.end() ) ;
        }
    }
    
    List get(){
        int ngroups = map.size() ;
        List indices(ngroups) ;
        
        Map::const_iterator it = map.begin() ;
        for( int i=0; i<ngroups; i++, ++it){
            indices[i] = it->second ;
        }
        return indices ;
        
    }
    
} ;
```

Using a concurrent map from tbb
===============================

The previous example trained a hash map in each thread, then merged
the maps together. The idea behind this implementation is to use
a thread safe hash map. We use : 

```cpp
typedef tbb::concurrent_unordered_map<
    int, tbb::concurrent_vector<int>, 
    VisitorSetHasher<Visitors>, 
    VisitorSetEqualPredicate<Visitors> 
> ConcurrentMap ;
```

Note that even though the `concurrent_unordered_map` is thread safe, 
we still need to also use a thread safe data structure for each individual 
value, hence the use of `tbb::concurrent_vector` and even there, I'm not sure we
are fully safe. 

The idea is that these thread safe data structures will do the necessary 
synchronisation for us so that we don't need to join at the end. Consequently the
implementation is simpler. 

```cpp
struct IndexMaker2 : public Worker {
    ConcurrentMap& map ;
    
    IndexMaker2( ConcurrentMap& map_ ) : 
        map(map_){}
        
    IndexMaker2( const IndexMaker2& other, Split) : map(other.map){}
        
    void operator()(std::size_t begin, std::size_t end) {
        for( size_t i =begin; i<end; i++) {
            map[i].push_back(i) ;
        }
    }
    
    List get(){
        int ngroups = map.size() ;
        List indices(ngroups) ;
        
        ConcurrentMap::const_iterator it = map.begin() ;
        for( int i=0; i<ngroups; i++, ++it){
            indices[i] = wrap( it->second.begin(), it->second.end() ) ;
        }
        return indices ;
        
    }
    
} ;

// [[Rcpp::export]]
List make_index_concurrent_hash_map( DataFrame data, CharacterVector by ){
    
    int n = data.nrows() ;
    
    Visitors visitors(data, by) ;
    VisitorSetHasher<Visitors> hasher(visitors) ; 
    VisitorSetEqualPredicate<Visitors> equal(visitors) ;
    ConcurrentMap map(1024, hasher, equal) ;
    
    IndexMaker2 indexer(map) ;
    
    parallelFor(0, n, indexer) ;
    
    return indexer.get() ;
}
```

Using manual threads
====================

Things like `parallelReduce` and `parallelFor` are nice abstractions which 
isolate us from handling the threads manually. However, we then do not 
know how many threads are used, we have no control over how the data is 
split, etc ... we just have to trust tbb to do the right thing. 

This next implementation uses threads manually to reclaim that control. We use
threads as given by `TinyThread` from `RcppParallel`, this is a lot less nice
than if we had C++11 standard support for threads, but that's close enough
and that will work on C++98. 

We split the data into as many threads as the available hardware concurrency, 
usually two times the number of cores with hyperthreading, and send roughly the 
same data size to each thread. The code for a thread: 

```cpp
struct Index3Thread {
public: 
    IndexRange range ;
    Visitors visitors;
    VisitorSetHasher<Visitors> hasher ; 
    VisitorSetEqualPredicate<Visitors> equal ;
    Map map ;
    MergeMap& merge_map ;
    
    Index3Thread( IndexRange range_, DataFrame data, CharacterVector by, MergeMap& merge_map_ ) : 
        range(range_), visitors(data, by), hasher(visitors), equal(visitors), 
        map(1024, hasher, visitors), 
        merge_map(merge_map_){}
                      
    void process(){
        size_t e = range.end() ;
        for( size_t i = range.begin(); i<e; i++) map[i].push_back(i) ;   
        
        Map::const_iterator it = map.begin() ;
        Map::const_iterator end = map.end() ;
        for( ; it != end ; ++it ){
            merge_map[it->first].push_back( &it->second ) ;    
        }
    }
} ;
```

The `process` method is the workhorse. It first trains an independent 
hash map, then merges this map with a thread safe map of this class: 

```cpp
typedef tbb::concurrent_unordered_map<
  int, 
  tbb::concurrent_vector<const std::vector<int>*>, 
  VisitorSetHasher<Visitors>, 
  VisitorSetEqualPredicate<Visitors>
> MergeMap ;
```

The idea is once we've trained each independent hash map, we don't want to
grow vectors for each group, we just collect pointers to already collected
indices. The main function is slightly more involved (mainly because of noise
imposed by non C++11 threads), but is typical low level parallel code using threads: 

```cpp
// [[Rcpp::export]]
List make_index_threads_unorderedmap_joinConcurrentMap( DataFrame data, CharacterVector by ){
    using namespace tthread;
      
    IndexRange inputRange(0, data.nrows());
    std::vector<IndexRange> ranges = splitInputRange(inputRange, 1);
    
    Visitors visitors(data, by);
    VisitorSetHasher<Visitors> hasher(visitors) ; 
    VisitorSetEqualPredicate<Visitors> equal(visitors) ;
    
    MergeMap merge_map(1024, hasher, equal) ;
    
    std::vector<thread*> threads;
    std::vector<Index3Thread*> workers ;
    for (std::size_t i = 0; i<ranges.size(); ++i) {
        Index3Thread* w = new Index3Thread(ranges[i], data, by, merge_map) ;
        workers.push_back(w) ;
        threads.push_back(new thread(process_thread<Index3Thread>, w));   
    }
    
    for (std::size_t i = 0; i<threads.size(); ++i) {
       threads[i]->join();
    }
    
    int nout = merge_map.size() ;
    List out(nout) ;
    MergeMap::const_iterator it = merge_map.begin() ;
    for( int i=0; i<nout; i++, ++it){ 
        const tbb::concurrent_vector<const std::vector<int>*>& chunks = it->second;
        int nv = chunks.size() ;
        int m = 0 ;
        for( int j=0; j<nv; j++) m += chunks[j]->size() ;
        IntegerVector ind = no_init(m) ;
        int k=0;
        int* p = ind.begin() ;
        for( int j=0; j<nv; j++){
            std::copy( chunks[j]->begin(), chunks[j]->end(), p ) ;
            p += chunks[j]->size() ;
        }
        out[i] = ind ;
    }
    
    for (std::size_t i = 0; i<threads.size(); ++i) {
       delete threads[i];
       delete workers[i];
    }
    
    return out ;
}
```

Example data
============

For these timings, we'll use the `babynames::babynames` dataset, and we will
group on variables `year` and `sex`. 

The `benchmark_make_index` function use `microbenchmark` to run 
each implementation several times: 


```r
benchmark_make_index( babynames, c('year', 'sex' ), times = 5L )
```

```
## Unit: milliseconds
##            expr   min    lq median    uq   max neval
##          serial 60.13 61.87  62.30 63.69 73.07     5
##  parallelReduce 15.92 19.14  19.15 19.22 19.95     5
##      concurrent 80.67 85.07  87.06 88.59 89.01     5
##         threads 19.92 20.04  20.22 20.71 34.48     5
```

Details
=======

Each of the implementations has a detailed version tracking steps, in order to 
see what each thread is doing throughout the execution. 



## serial implementation

![plot of chunk unnamed-chunk-4](figure/unnamed-chunk-4.png) 

## parallelReduce

![plot of chunk unnamed-chunk-5](figure/unnamed-chunk-5.png) 

## conrcurrent map

![plot of chunk unnamed-chunk-6](figure/unnamed-chunk-6.png) 

## manual threads, joining with a concurrent map

![plot of chunk unnamed-chunk-7](figure/unnamed-chunk-7.png) 

Conclusions
===========


This is work in progress, other implementations are coming. A few things: 

 - `parallelReduce` and `parallelFor` seem to split the data into a lot of threads, many of which only then operate on very small subset of data. This might have implications later on what each worker does. 
 - in this example, the concurrent data structure perform really badly.
 - training each map is what takes time. The joining then is fast. This might be related to the data set used. Joining might be more expensive when each of the maps have many groups. 
 
We need more examples



