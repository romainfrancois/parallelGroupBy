#include <parallelGroupBy.h> 

// [[Rcpp::export]]
List make_index6( DataFrame data, CharacterVector by ){
    // TimeTracker tracker ;
    // tracker.step("start") ;
    
    int nr = data.nrows() ;
    std::vector<size_t> hashes(nr) ;
    CalculateHash calc(data, by, hashes) ;
    parallelFor( 0, nr, calc ) ;
    
    // tracker.step( "hashes" ) ;
    
    Visitors visitors(data, by) ;
    VisitorSetEqualPredicate<Visitors> equal(visitors) ;
    DummyHasher hasher(hashes) ;
    
    Map2 map(1024, hasher, equal);  
    for( int i=0; i<nr; i++)
        map[i].push_back(i) ;
    
    // tracker.step( "train" );
    
    int ngroups = map.size() ;
    List indices(ngroups) ;
    
    Map2::const_iterator it = map.begin() ;
    for( int i=0; i<ngroups; i++, ++it){
        indices[i] = it->second ;
    }
    
    // tracker.step( "collect" ) ;
    
    // return List::create( (SEXP)tracker, indices ) ;
    return List::create( indices ) ;
}

// [[Rcpp::export]]
List make_index7( DataFrame data, CharacterVector by ){
    // TimeTracker tracker ;
    // tracker.step("start") ;
    
    int nr = data.nrows() ;
    std::vector<size_t> hashes(nr) ;
    CalculateHash calc(data, by, hashes) ;
    parallelFor( 0, nr, calc ) ;
    
    // tracker.step( "hashes" ) ;
    
    Visitors visitors(data, by) ;
    VisitorSetEqualPredicate<Visitors> equal(visitors) ;
    DummyHasher hasher(hashes) ;
    typedef boost::unordered_map<int, int, DummyHasher, VisitorSetEqualPredicate<Visitors> > iMap ;
    iMap m(1024, hasher, equal) ;
    for(int i=0; i<nr; i++){
        m[i]++ ;            
    }
    
    // tracker.step( "train count map" ) ;
    
    int ngroups = m.size() ;
    IntegerVector groups = no_init(nr) ;
    IntegerVector group_sizes = no_init(ngroups);
    
    iMap::const_iterator it = m.begin() ;
    for( int i=0; i<ngroups; i++, ++it){
        group_sizes[i] = it->second ;
    }
    
    // tracker.step( "init" ) ;
    
    // return List::create( (SEXP)tracker, groups, group_sizes ) ;
    return List::create( groups, group_sizes ) ;
}



/*** R
    require(dplyr)
    require(babynames)
    require(microbenchmark)
    require(RcppParallel)
    
    force(babynames)
    
    reorg <- function(x){
        x <- lapply(x, sort)
        o <- order( sapply(x, "[[", 1 ) )
        x[o]
    }
    
    # res_serial <- reorg(make_index_serial( babynames, c("year", "sex" ) ))
    # res_parallelReduce <- reorg(make_index_parallel( babynames, c("year", "sex" ) ))
    # res_concurrent <- reorg(make_index_concurrent_hash_map( babynames, c("year", "sex" ) ))
    # res3 <- reorg(make_index3( babynames, c("year", "sex" ) ))
    
    # identical( res_serial, res_parallelReduce )
    # identical( res_serial, res_concurrent )
    # identical( res_serial, res3 )
    # 
    microbenchmark( 
        # dplyr  = dplyr::group_by(babynames, sex, name) , 
        serial = make_index_serial( babynames, c("year", "sex" ) ), 
        parallelReduce = make_index_parallel( babynames, c("year", "sex" ) ),
        tbb_concurrent = make_index_concurrent_hash_map( babynames, c("year", "sex" ) ),
        impl3 = make_index3( babynames, c("year", "sex" ) ),
        impl4 = make_index4( babynames, c("year", "sex" ) ),
        times  = 2
    )
    
    # res1 <- make_index_serial( babynames, c("year", "sex" ) )
    
    # f <- function(times){
    #     times <- ( times - times[1] ) / 1e6
    #     times
    # }
    # 
    # measure <- function(fun) {
    #     t(sapply(1:5, function(i){
    #         res6 <- fun( babynames, c("year", "sex" ) )
    #         f(res6[[1]])
    #     }))
    # }
    # 
    # measure(make_index_serial)
    # measure(make_index7)
    # 
    # 
    # data <- data.frame( x = c(1,1,2,2), y = c(1,1,2,2) )
    # make_index7( data, c("x", "y") )
    
    # res5[[2]]
    
*/

