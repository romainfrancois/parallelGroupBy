#include <parallelGroupBy.h> 

typedef tbb::concurrent_unordered_map<
  int, 
  tbb::concurrent_vector<const std::vector<int>*>, 
  VisitorSetHasher<Visitors>, 
  VisitorSetEqualPredicate<Visitors>
> MergeMap ;

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

struct inst_Index3Thread {
public: 
    IndexRange range ;
    Visitors visitors;
    VisitorSetHasher<Visitors> hasher ; 
    VisitorSetEqualPredicate<Visitors> equal ;
    Map map ;
    MergeMap& merge_map ;
    Timer& timer ;
    
    inst_Index3Thread( IndexRange range_, DataFrame data, CharacterVector by, MergeMap& merge_map_, Timer& timer_ ) : 
        range(range_), visitors(data, by), hasher(visitors), equal(visitors), 
        map(1024, hasher, visitors), 
        merge_map(merge_map_), 
        timer(timer_) {}
                      
    void process(){
        timer.step( "start" ) ;
        size_t e = range.end() ;
        for( size_t i = range.begin(); i<e; i++) map[i].push_back(i) ;   
        timer.step( "train" ) ;
        
        Map::const_iterator it = map.begin() ;
        Map::const_iterator end = map.end() ;
        for( ; it != end ; ++it ){
            merge_map[it->first].push_back( &it->second ) ;    
        }
        timer.step( "merge" ) ;
    }
} ;


// [[Rcpp::export]]
List detail_make_index_threads_unorderedmap_joinConcurrentMap( DataFrame data, CharacterVector by ){
    using namespace tthread;
      
    IndexRange inputRange(0, data.nrows());
    std::vector<IndexRange> ranges = splitInputRange(inputRange, 1);
    
    int nthreads = ranges.size() ;
    FixedSizeTimers<Timer> timers(nthreads+1) ;
    Timer& timer = timers.get(0) ;
    timer.step( "start" );
    
    Visitors visitors(data, by);
    VisitorSetHasher<Visitors> hasher(visitors) ; 
    VisitorSetEqualPredicate<Visitors> equal(visitors) ;
    
    MergeMap merge_map(1024, hasher, equal) ;
    
    std::vector<thread*> threads;
    std::vector<inst_Index3Thread*> workers ;
    for (std::size_t i = 0; i<ranges.size(); ++i) {
        inst_Index3Thread* w = new inst_Index3Thread(ranges[i], data, by, merge_map, timers.get(i+1) ) ;
        workers.push_back(w) ;
        threads.push_back(new thread(process_thread<inst_Index3Thread>, w));   
    }
    timer.step( "spawn" ) ;
    
    for (std::size_t i = 0; i<nthreads; ++i) {
       threads[i]->join();
       timer.step( "join" ) ;
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
    timer.step( "structure" ) ;
    
    return List::create( (SEXP)timers, out ) ;
}



