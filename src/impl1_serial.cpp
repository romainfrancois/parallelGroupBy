#include <parallelGroupBy.h> 

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

// [[Rcpp::export]]
List detail_make_index_impl1_serial( DataFrame data, CharacterVector by ){
    Timer timer ;
    timer.step("start") ;
    
    int n = data.nrows() ;
    Visitors visitors(data, by) ;
    VisitorSetHasher<Visitors> hasher(visitors); 
    VisitorSetEqualPredicate<Visitors> equal(visitors);
    
    Map map(1024, hasher, equal);  
    for( int i=0; i<n; i++)
        map[i].push_back(i) ;
    timer.step("train") ;
    
    int ngroups = map.size() ;
    List indices(ngroups) ;
    
    Map::const_iterator it = map.begin() ;
    for( int i=0; i<ngroups; i++, ++it){
        indices[i] = it->second ;
    }
    timer.step("collect") ;
    
    return List::create( (SEXP)timer, indices ) ;
}

