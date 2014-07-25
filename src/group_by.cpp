#include <Rcpp.h>
#include <boost/unordered_map.hpp>
#include <boost/unordered_set.hpp>
#include <RcppParallel.h>
#include <Rcpp/Benchmark/Timer.h>

// [[Rcpp::depends(BH,RcppParallel)]]

using namespace Rcpp ;
using namespace RcppParallel;

class TimeTracker { 
public:              
    TimeTracker(){}
    
    inline operator SEXP(){
        size_t n = steps.size();
        NumericVector out(n);
        CharacterVector names(n);
        for (size_t i=0; i<n; i++) {
            names[i] = steps[i].first;
            out[i] = steps[i].second;
        }
        out.attr("names") = names;
        return out;   
    }
    
    void step( const char* name ){
        steps.push_back( std::make_pair( name, (double)get_nanotime() ) ) ;    
    }
    
private:
    std::vector<std::pair<std::string,double> > steps ;    
    
} ;

namespace dplyr{  

    template <int RTYPE>
    struct comparisons {
        typedef typename Rcpp::traits::storage_type<RTYPE>::type STORAGE ;
        
        inline bool is_equal(STORAGE lhs, STORAGE rhs ) const {
            return lhs == rhs ;    
        }
        
        inline bool equal_or_both_na( STORAGE lhs, STORAGE rhs ) const {
            return lhs == rhs ;    
        }
        
        inline bool is_na(STORAGE x) const { 
            return Rcpp::traits::is_na<RTYPE>(x); 
        }
        
    } ;  
    
    template <>
    struct comparisons<STRSXP> {
        inline bool is_equal(SEXP lhs, SEXP rhs ) const {
            return lhs == rhs ;    
        }
        
        inline bool equal_or_both_na( SEXP lhs, SEXP rhs ) const {
            return lhs == rhs ;    
        }
        
        inline bool is_na(SEXP x) const { 
            return Rcpp::traits::is_na<STRSXP>(x); 
        }
        
    } ;
    
    // taking advantage of the particularity of NA_REAL
    template <>
    struct comparisons<REALSXP> {
        inline bool is_equal(double lhs, double rhs ) const {
            return lhs == rhs ;    
        }
        
        inline bool equal_or_both_na( double lhs, double rhs ) const {
            return lhs == rhs || ( is_na(lhs) && is_na(rhs) );    
        }      
        
        inline bool is_na(double x) const { 
            return Rcpp::traits::is_na<REALSXP>(x); 
        }
     
    } ;

    
    class VectorVisitor {
    public:
        virtual ~VectorVisitor(){}
        
        /** hash the element of the visited vector at index i */
        virtual size_t hash(int i) const = 0 ;
        
        /** are the elements at indices i and j equal */
        virtual bool equal(int i, int j) const = 0 ;
    
        virtual int size() const = 0 ;
        
        virtual VectorVisitor* clone() const = 0 ;
    } ;
    
    template <typename Container>
    inline int output_size( const Container& container){
        return container.size() ;
    }
    
    template <>
    inline int output_size<LogicalVector>( const LogicalVector& container){
        return std::count( container.begin(), container.end(), 1 ) ;
    }
    
    template <int RTYPE> std::string VectorVisitorType() ;
    template <> inline std::string VectorVisitorType<INTSXP>() { return "integer" ; }
    template <> inline std::string VectorVisitorType<REALSXP>(){ return "numeric" ; }
    template <> inline std::string VectorVisitorType<LGLSXP>() { return "logical" ; }
    template <> inline std::string VectorVisitorType<STRSXP>() { return "character" ; }
    
    /** 
     * Implementations 
     */
    template <int RTYPE>
    class VectorVisitorImpl : public VectorVisitor, public comparisons<RTYPE> {
    public:
        typedef comparisons<RTYPE> compare ;
        typedef Rcpp::Vector<RTYPE> VECTOR ;
        
        typedef typename Rcpp::traits::storage_type<RTYPE>::type STORAGE ;
        typedef std::hash<STORAGE> hasher ;
        
        VectorVisitorImpl( const VECTOR& vec_ ) : vec(vec_) {}
            
        size_t hash(int i) const { 
            return hash_fun( vec[i] ) ;
        } 
        inline bool equal(int i, int j) const { 
            return compare::is_equal( vec[i], vec[j] ) ;
        }
        
        int size() const { return vec.size() ; }
        
        VectorVisitor* clone() const {
            return new VectorVisitorImpl(vec) ;   
        }
        
    protected: 
        VECTOR vec ;
        hasher hash_fun ;
        
    } ;
            
    inline VectorVisitor* visitor( SEXP vec ){
        switch( TYPEOF(vec) ){
            case INTSXP:  return new VectorVisitorImpl<INTSXP>( vec ) ;
            case REALSXP: return new VectorVisitorImpl<REALSXP>( vec ) ;
            case LGLSXP:  return new VectorVisitorImpl<LGLSXP>( vec ) ;
            case STRSXP:  return new VectorVisitorImpl<STRSXP>( vec ) ;
            default: break ;
        }
        
        // should not happen
        return 0 ;
    }

    class Visitors {
    public:
        Visitors( const DataFrame& df, const CharacterVector& names) : nv(names.size()), visitors(nv){
            for( int i=0; i<nv; i++){
                String name = names[i] ;
                visitors[i] = visitor( df[name] ) ;    
            }
        }
        
        Visitors(const Visitors& other){
            nv = other.nv ;
            for( int i=0; i<nv; i++) {
                visitors.push_back( visitors[i]->clone() ) ;
            }
            
        }
        
        size_t hash( int j) const {
            size_t seed = visitors[0]->hash(j) ;
            for( int k=1; k<nv; k++){
                hash_combine( seed, visitors[k]->hash(j) ) ;
            }              
            return seed ;
        }
        
        bool equal( int i, int j) const {
            if( i == j ) return true ;
            for( int k=0; k<nv; k++)
                if( ! visitors[k]->equal(i,j) ) return false ;    
            return true ;
        }
        
        inline int size() const {
            return nv ;    
        }
        
    private:
        int nv ; 
        std::vector<VectorVisitor*> visitors ;
        
        inline void hash_combine(size_t & seed, size_t hash) const {
            seed ^= hash + 0x9e3779b9 + (seed << 6) + (seed >> 2);    
        }
    
    } ;
    
    template <typename VisitorSet>
    class VisitorSetHasher {
    public:
        VisitorSetHasher() : visitors(0){}
        
        VisitorSetHasher( const VisitorSet& visitors_ ) : visitors(visitors_){} ;
        inline size_t operator()(int i) const {
            return visitors.hash(i) ;
        }
        
    private:
        const VisitorSet& visitors ;  
    } ;
    
    template <typename VisitorSet>
    class VisitorSetEqualPredicate {
    public:
        VisitorSetEqualPredicate() : visitors(0){}
        
        VisitorSetEqualPredicate( const VisitorSet& visitors_ ) : visitors(visitors_) {} ;
        inline bool operator()(int i, int j) const {
            return visitors.equal(i,j) ;
        }
        
    private:
        const VisitorSet& visitors ;  
    } ;
}

using namespace dplyr ;
typedef boost::unordered_map<int, std::vector<int>, 
    VisitorSetHasher<Visitors>, 
    VisitorSetEqualPredicate<Visitors> > Map ;

// [[Rcpp::export]]
List make_index_serial( DataFrame data, CharacterVector by ){
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

// ----------- using manual threads 1


typedef tbb::concurrent_unordered_map<
  int, 
  tbb::concurrent_vector<const std::vector<int>*>, 
  VisitorSetHasher<Visitors>, 
  VisitorSetEqualPredicate<Visitors>
> MergeMap ;


template <typename Work>
inline void index3_thread( void* data ){
    Work* work = reinterpret_cast<Work*>(data) ;
    work->process() ;    
}
       
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
List make_index3( DataFrame data, CharacterVector by ){
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
        threads.push_back(new thread(index3_thread<Index3Thread>, w));   
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


// ----------- using manual threads 2

template <typename Work>
inline void index4_thread( void* data ){
    Work* work = reinterpret_cast<Work*>(data) ;
    work->process() ;    
}
       
struct Index4Thread {
    IndexRange range ;
    Visitors visitors;
    VisitorSetHasher<Visitors> hasher ; 
    VisitorSetEqualPredicate<Visitors> equal ;
    Map map ;
    
    Index4Thread( IndexRange range_, DataFrame data, CharacterVector by) : 
        range(range_), visitors(data, by), hasher(visitors), equal(visitors), 
        map(1024, hasher, visitors){}
                      
    void process(){
        size_t e = range.end() ;
        for( size_t i = range.begin(); i<e; i++) map[i].push_back(i) ;   
    }
} ;

typedef boost::unordered_map<int, std::vector<int>, VisitorSetHasher<Visitors>, VisitorSetEqualPredicate<Visitors> > CountMap ;
    
// struct OuputThread4 {
//     Index4Thread& indexer ;
//     CountMap& count_map ;
//     
//     OuputThread4( int idx, Index4Thread& indexer_, CountMap& count_map_, List res ) : 
//         indexer(indexer_), count_map(count_map_) {}
//     
//     void process(){
//         size_t e = indexer.range.end() ;
//         for( size_t i = indexer.range.begin(); i < e; i++){
//             CountMap::const_iterator it = count_map.find(i) ;
//             std::vector<int>& positions = it->second ;
//             IntegerVector v = res[
//         }
//     }
//     
// } ;


// [[Rcpp::export]]
List make_index4( DataFrame data, CharacterVector by ){    
    using namespace tthread; 
          
    // TimeTracker tracker ;
    // tracker.step("start") ;
    
    IndexRange inputRange(0, data.nrows());
    std::vector<IndexRange> ranges = splitInputRange(inputRange, 1);
    int nthreads = ranges.size();
    
    // tracker.step("ranges") ;
    
    Visitors visitors(data, by);
    VisitorSetHasher<Visitors> hasher(visitors) ; 
    VisitorSetEqualPredicate<Visitors> equal(visitors) ;
    
    // tracker.step("visitors") ;
    
    std::vector<thread*> threads;
    std::vector<Index4Thread*> workers ;
    for (std::size_t i = 0; i<ranges.size(); ++i) {
        Index4Thread* w = new Index4Thread(ranges[i], data, by) ;
        workers.push_back(w) ;
        threads.push_back(new thread(index4_thread<Index4Thread>, w));   
    }
    // tracker.step("threads") ;
    
    CountMap count_map(1024, hasher, equal) ;
    
    for (std::size_t i = 0; i<threads.size(); ++i) {
       threads[i]->join();
       // tracker.step("join") ;
       
       Map& map = workers[i]->map ;
       Map::const_iterator start = map.begin() ;
       Map::const_iterator end   = map.end() ;
       for( ; start != end; ++start ){
            int key = start->first ;
            CountMap::iterator it = count_map.find(key) ;
            if( it == count_map.end() ){
                it = count_map.insert( std::make_pair(key, std::vector<int>(nthreads) ) ).first;
                (it->second)[i] = start->second.size() ;
            } else {
                std::vector<int>& v = it->second ;
                v[i] = v[i-1] + start->second.size() ;
            }    
       }
       // tracker.step("count_map") ;
       
    }
    
    int nout = count_map.size() ;
    List out(nout) ;
    CountMap::const_iterator count_it = count_map.begin() ;
    for(int i=0; i<nout; i++, ++count_it){
        out[i] = IntegerVector(count_it->second[nthreads-1]) ;
    }
    // tracker.step( "structure" );
    
    // std::vector<thread*> output_threads;
    // std::vector<OuputThread4*> output_workers ;
    // for (std::size_t i = 0; i<threads.size(); ++i) {
    //     OuputThread4* w = new OuputThread4() ;
    //     output_workers.push_back(w) ;
    //     threads.push_back(new thread(index4_thread<OuputThread4>, w));   
    // }      
    
    for (std::size_t i = 0; i<threads.size(); ++i) {
       delete threads[i];
       delete workers[i];
    }        
    
    // tracker.step( "delete" ) ;
                  
    // return List::create( (SEXP)tracker, out ) ;
    return out ;
}
   

struct CalculateHash : public Worker {
    Visitors visitors ;
    VisitorSetHasher<Visitors> hasher ; 
    
    std::vector<size_t>& hashes ; 
    
    CalculateHash(DataFrame data, CharacterVector by, std::vector<size_t>& hashes_) : 
        visitors(data,by), hasher(visitors), hashes(hashes_){}
        
    void operator()(std::size_t begin, std::size_t end) {
        for( int i=begin; i<end; i++){
            hashes[i] = hasher(i) ;
        }
    }
    
} ;     

// [[Rcpp::export]]
List make_index5( DataFrame data, CharacterVector by ){
    // TimeTracker tracker ;
    // tracker.step("start") ;
    
    int nr = data.nrows() ;
    std::vector<size_t> hashes(nr) ;
    CalculateHash calc(data, by, hashes) ;
    parallelFor( 0, nr, calc ) ;
    
    // tracker.step( "hashes" ) ;
    
    Visitors visitors(data, by) ;
    
    IntegerVector positions(nr) ; int* ptr = positions.begin() ;
    std::vector<int> group_sizes ;
    std::vector<size_t> group_hashes ;
    int ngroups = 0 ;
    
    // tracker.step( "data" ) ;
    
    for(int i=0; i<nr; i++){
        Rprintf( "-" ) ;
        size_t h = hashes[i] ;
        
        // find the first group with same hash and equal
        int pos = 0 ;
        int g = 0 ;
        for( ; g<ngroups; g++){ 
            pos += group_sizes[g] ;
            
            if( group_hashes[g] == h && visitors.equal(ptr[pos], i) ) {
                std::memmove( 
                    ptr + pos + 1, 
                    ptr + pos, 
                    sizeof(int) * ( i - pos )
                    ) ;  
                
                ptr[pos] = i ;
                group_sizes[g]++ ;
                break ; 
            }
        }
        
        if( g == ngroups ){
            // reach the end, just create a new group
            group_hashes.push_back(h) ;
            group_sizes.push_back(1) ;
            ptr[pos] = i ;
            ngroups++ ;
        }
        
    }
    
    // tracker.step( "populate" ) ;
    
    // return List::create( (SEXP)tracker, positions, wrap(group_sizes), wrap(hashes) ) ;
    return List::create( positions ) ; 
}       
                  
struct DummyHasher {
    std::vector<size_t> data ;
    DummyHasher( std::vector<size_t> data_ ) : data(data_){} 
    inline size_t operator()(int i) const {
        return data[i] ;
    }  
} ;

typedef boost::unordered_map<int, std::vector<int>, 
    DummyHasher, 
    VisitorSetEqualPredicate<Visitors> > Map2 ;

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
 
// struct CalculateHash : public Worker {
//     Visitors visitors ;
//     VisitorSetHasher<Visitors> hasher ; 
//     
//     std::vector<size_t>& hashes ; 
//     
//     CalculateHash(DataFrame data, CharacterVector by, std::vector<size_t>& hashes_) : 
//         visitors(data,by), hasher(visitors), hashes(hashes_){}
//         
//     void operator()(std::size_t begin, std::size_t end) {
//         for( int i=begin; i<end; i++){
//             hashes[i] = hasher(i) ;
//         }
//     }
//     
// } ;     
// 
// typedef tbb::concurrent_unordered_map<int, std::vector<int>, DummyHasher, VisitorSetEqualPredicate<Visitors> > GroupSizeMap ; 
// 
// struct CountGroupSize : public Worker{
//     GroupSizeMap& map ;
//     
//     CountGroupSize( GroupSizeMap& map_ ) : map(map_){}
//     CountGroupSize( const CountGroupSize& other, Split ) : map(other.map){}
//     
//     void operator()(std::size_t begin, std::size_t end) {
//         for( size_t i =begin; i<end; i++) {
//             if( map.find(i) ){
//                     
//             }
//         }
//     }
//     
// } ;


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

