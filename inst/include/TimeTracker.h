#ifndef parallelGroupBy_TimeTracker_H
#define parallelGroupBy_TimeTracker_H

#if defined(_WIN32)
    #define WIN32_LEAN_AND_MEAN
    #include <windows.h>
#elif defined(__APPLE__)
    #include <mach/mach_time.h>
#elif defined(linux) || defined(__linux) || defined(__FreeBSD__) || defined(__NetBSD__) || defined(__OpenBSD__) || defined(__GLIBC__) || defined(__GNU__) || defined(__CYGWIN__)
    #include <time.h>
#elif defined(sun) || defined(__sun) || defined(_AIX)
    #include <sys/time.h>
#else /* Unsupported OS */
    #error "Rcpp::Timer not supported by your OS."
#endif

namespace Rcpp{

    typedef uint64_t nanotime_t;

#if defined(_WIN32)

    inline nanotime_t get_nanotime(void) {
        LARGE_INTEGER time_var, frequency;
        QueryPerformanceCounter(&time_var);
        QueryPerformanceFrequency(&frequency);

        /* Convert to nanoseconds */
        return 1.0e9 * time_var.QuadPart / frequency.QuadPart;
    }

#elif defined(__APPLE__)

    inline nanotime_t get_nanotime(void) {
        nanotime_t time;
        mach_timebase_info_data_t info;

        time = mach_absolute_time();
        mach_timebase_info(&info);

        /* Convert to nanoseconds */
        return time * (info.numer / info.denom);
    }

#elif defined(linux) || defined(__linux) || defined(__FreeBSD__) || defined(__NetBSD__) || defined(__OpenBSD__) || defined(__GLIBC__) || defined(__GNU__) || defined(__CYGWIN__)

    static const nanotime_t nanoseconds_in_second = static_cast<nanotime_t>(1000000000.0);

    inline nanotime_t get_nanotime(void) {
        struct timespec time_var;

        /* Possible other values we could have used are CLOCK_MONOTONIC,
         * which is takes longer to retrieve and CLOCK_PROCESS_CPUTIME_ID
         * which, if I understand it correctly, would require the R
         * process to be bound to one core.
         */
        clock_gettime(CLOCK_REALTIME, &time_var);

        nanotime_t sec = time_var.tv_sec;
        nanotime_t nsec = time_var.tv_nsec;

        /* Combine both values to one nanoseconds value */
        return (nanoseconds_in_second * sec) + nsec;
    }

#elif defined(sun) || defined(__sun) || defined(_AIX)

    /* short an sweet! */
    inline nanotime_t get_nanotime(void) {
        return gethrtime();
    }

#endif
    
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


}

#endif
