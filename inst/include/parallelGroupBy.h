#ifndef parallelGroupBy_parallelGroupBy_H
#define parallelGroupBy_parallelGroupBy_H

#include <Rcpp.h>
#include <RcppParallel.h>

using namespace Rcpp ;
using namespace RcppParallel;

#include <TimeTracker.h>
#include <boost/unordered_map.hpp>
#include <boost/unordered_set.hpp>
#include <mini_dplyr.h>

using namespace dplyr ;

typedef boost::unordered_map<int, std::vector<int>, 
    VisitorSetHasher<Visitors>, 
    VisitorSetEqualPredicate<Visitors> > Map ;

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

    
#endif
