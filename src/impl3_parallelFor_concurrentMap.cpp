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
    
    void join(const IndexMaker2& ){}
    
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

// [[Rcpp::export]]
List detail_make_index_concurrent_hash_map( DataFrame data, CharacterVector by ){
    Timers timers ;
    ProportionTimer<Timer>& timer = timers.front() ;
    timer.step( "start" ) ;
    int n = data.nrows() ;
    
    Visitors visitors(data, by) ;
    VisitorSetHasher<Visitors> hasher(visitors) ; 
    VisitorSetEqualPredicate<Visitors> equal(visitors) ;
    ConcurrentMap map(1024, hasher, equal) ;
       
    IndexMaker2 indexer(map) ;
    TimedReducer<IndexMaker2, Timer, tbb::mutex, tbb::mutex::scoped_lock> timed_indexer(indexer, timers) ;
                      
    parallelReduce(0, n, timed_indexer, 1000) ;
    timer.step( "parallelReduce" ) ;
    List out = timed_indexer.get() ;
    timer.step( "structure" );
    return out ;
}

