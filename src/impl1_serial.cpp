#include <parallelGroupBy.h> 

// [[Rcpp::export]]
List make_index_impl1_serial( DataFrame data, CharacterVector by ){
    // TimeTracker tracker ;
    // tracker.step("start") ;
    
    int n = data.nrows() ;
    Visitors visitors(data, by) ;
    VisitorSetHasher<Visitors> hasher(visitors); 
    VisitorSetEqualPredicate<Visitors> equal(visitors);
    
    Map map(1024, hasher, equal);  
    for( int i=0; i<n; i++)
        map[i].push_back(i) ;
    // tracker.step("train") ;
    
    int ngroups = map.size() ;
    List indices(ngroups) ;
    
    Map::const_iterator it = map.begin() ;
    for( int i=0; i<ngroups; i++, ++it){
        indices[i] = it->second ;
    }
    // tracker.step("collect") ;
    
    // return List::create( (SEXP)tracker, indices ) ;
    return indices ;
}

