#include <parallelGroupBy.h> 

typedef tbb::concurrent_unordered_map<
    int, tbb::concurrent_vector<int>, 
    VisitorSetHasher<Visitors>, 
    VisitorSetEqualPredicate<Visitors> 
> ConcurrentMap ;


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

struct TimedIndexMaker2 : public Worker {
    ConcurrentMap& map ;
    Timers& timers ;
    Timer& timer ;
    
    TimedIndexMaker2( ConcurrentMap& map_, Timers& timers_ ) : 
        map(map_), timers(timers_), timer(timers.get_new_timer()) 
    {}
        
    TimedIndexMaker2( const TimedIndexMaker2& other, Split) : 
        map(other.map), timers(other.timers), timer(timers.get_new_timer()) 
    {}
        
    void operator()(std::size_t begin, std::size_t end) {
        timer.step( "start") ;
        for( size_t i =begin; i<end; i++) {
            map[i].push_back(i) ;
        }
        timer.step("train") ;
    }
    
    List get(){
        timer.step( "start" ) ;
        int ngroups = map.size() ;
        List indices(ngroups) ;
        
        ConcurrentMap::const_iterator it = map.begin() ;
        for( int i=0; i<ngroups; i++, ++it){
            indices[i] = wrap( it->second.begin(), it->second.end() ) ;
        }
        timer.step("structure");
        return indices ;
        
    }
    
    void join( const TimedIndexMaker2& ){}
    
} ;


// [[Rcpp::export]]
List detail_make_index_concurrent_hash_map( DataFrame data, CharacterVector by ){
    Timers timers ;
    Timer& timer = timers.get_new_timer() ;
    timer.step( "start" ) ;
    int n = data.nrows() ;
    
    Visitors visitors(data, by) ;
    VisitorSetHasher<Visitors> hasher(visitors) ; 
    VisitorSetEqualPredicate<Visitors> equal(visitors) ;
    ConcurrentMap map(1024, hasher, equal) ;
    
    TimedIndexMaker2 indexer(map, timers) ;
    
    // for some reason I get segfaults with parallelFor
    parallelReduce(0, n, indexer) ;
    timer.step( "parallelReduce" ) ;
    
    List res = indexer.get() ;
    
    timer.step( "structure" ) ;
    
    return List::create( (SEXP)timers, res ) ;
}

