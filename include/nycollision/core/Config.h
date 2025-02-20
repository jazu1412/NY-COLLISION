#pragma once
#include <iostream>

#ifdef _OPENMP
#include <omp.h>
#endif

namespace nycollision {
namespace config {

// OpenMP configuration
constexpr int NUM_THREADS = 7;

inline void initializeOpenMP() {
    #ifdef _OPENMP
    std::cout << "Setting thread count in OpenMP...\n";
    omp_set_num_threads(NUM_THREADS);
    #pragma omp parallel
    {
        #pragma omp master
        {
            std::cout << "Inside parallel region: " 
                      << omp_get_num_threads() 
                      << " threads\n";
        }
    }
    #endif
}

} // namespace config
} // namespace nycollision
