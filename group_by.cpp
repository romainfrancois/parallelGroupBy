#include <Rcpp.h>
#include <boost/unordered_map.hpp>
#include <RcppParallel.h>

// [[Rcpp::depends(BH,RcppParallel)]]

using namespace Rcpp ;
using namespace RcppParallel;

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
        
        return List::create(1) ;
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




/*** R
    require(dplyr)
    require(babynames)
    require(microbenchmark)
    require(RcppParallel)
    
    force(babynames)
    
    microbenchmark( 
        # dplyr  = dplyr::group_by(babynames, sex, name) , 
        serial = make_index_serial( babynames, c("sex", "name") ), 
        parallelReduce = make_index_parallel( babynames, c("sex", "name") ),
        times  = 5
    )
    
*/

