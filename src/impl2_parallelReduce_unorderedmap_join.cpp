#include <parallelGroupBy.h> 

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

// [[Rcpp::export]]
List make_index_parallel( DataFrame data, CharacterVector by ){
    
    int n = data.nrows() ;
    
    Visitors visitors(data, by) ;
    IndexMaker indexer(visitors) ;
    
    parallelReduce(0, n, indexer) ;
    
    return indexer.get() ;
}
    
struct TimedIndexMaker : public Worker {
    Visitors& visitors ;
    VisitorSetHasher<Visitors> hasher ; 
    VisitorSetEqualPredicate<Visitors> equal ;
    Map map ;
    Timers& timers ;
    Timer& timer ;
    
    TimedIndexMaker( Visitors& visitors_, Timers& timers_ ) : 
        visitors(visitors_), 
        hasher(visitors), 
        equal(visitors), 
        map(1024, hasher, equal),
        timers(timers_),
        timer(timers.get_new_timer())
    {}
    
    TimedIndexMaker( const TimedIndexMaker& other, Split) : 
        visitors(other.visitors), 
        hasher(visitors), 
        equal(visitors), 
        map(1024, hasher, equal), 
        timers(other.timers), 
        timer(timers.get_new_timer())
    {}
        
    void operator()(std::size_t begin, std::size_t end) {
        timer.step( "start" );
        for( size_t i =begin; i<end; i++) map[i].push_back(i) ;
        timer.step( "train" ) ;
    }
    
    void join(const TimedIndexMaker& rhs) {
        timer.step( "start" ) ;
        // join data from rhs into this. 
        Map::const_iterator it = rhs.map.begin() ;
        for( ; it != rhs.map.end(); ++it ){
            // find if it exist in this map
            std::vector<int>& v = map[it->first] ;
            
            v.insert( v.end(), it->second.begin(), it->second.end() ) ;
        }
        timer.step( "join" ) ;
    }
    
    List get(){
        timer.step( "start" ) ;
        int ngroups = map.size() ;
        List indices(ngroups) ;
        
        Map::const_iterator it = map.begin() ;
        for( int i=0; i<ngroups; i++, ++it){
            indices[i] = it->second ;
        }
        timer.step( "structure" ) ;
        
        return indices ;
    }
    
} ;


// [[Rcpp::export]]
List detail_make_index_parallel( DataFrame data, CharacterVector by ){
    Timers timers ;
    Timer& timer = timers.get_new_timer() ;
    timer.step("start") ;
    
    int n = data.nrows() ;
    
    Visitors visitors(data, by) ;
    TimedIndexMaker indexer(visitors, timers) ;
    
    parallelReduce(0, n, indexer) ;
    timer.step( "parallelReduce" ) ;
    
    List res = indexer.get() ;
    
    timer.step( "structure" ) ;
    
    return List::create( (SEXP)timers, res ) ;
}

