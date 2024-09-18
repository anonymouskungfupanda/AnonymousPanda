#pragma once

#include <iostream>
#include <cstdint>
#include <cassert>
#include <chrono>


/* Uncomment to enable runtime analysis */
// #define RT_ANALYSIS

#ifdef RT_ANALYSIS
#define RT_START(name)  rt_analysis.name.start()
#define RT_STOP(name)   rt_analysis.name.stop()
#define RT_REPORT() \
    do {                                                                                                    \
        std::cout << "-------------------- Runtime Analysis Report --------------------" << std::endl;      \
        std::cout << "(ns) setup:        " << std::fixed << rt_analysis.setup.get() << "\n";                \
        std::cout << "(ns) cache_hit:    " << std::fixed << rt_analysis.cache_hit.get() << "\n";            \
        std::cout << "(ns) cache_miss:   " << std::fixed << rt_analysis.cache_miss.get() << "\n";           \
        std::cout << "(ns) gc_select:    " << std::fixed << rt_analysis.gc_select.get() << "\n";            \
        std::cout << "(ns) gc_reclaim:   " << std::fixed << rt_analysis.gc_reclaim.get() << "\n";           \
    } while(0)
#else
#define RT_START(...)
#define RT_STOP(...)
#define RT_REPORT(...)     
#endif /* RT_ANALYSIS */


/* For keeping track of a particular runtime measurement */
struct rt_measurement {
    double get() {
        return (double)total_duration / (double)occurrences;
    }
    void start() {
        using namespace std::chrono;
        assert(!open); open = true;
        timestamp = high_resolution_clock::now();
    }
    void stop() {
        using namespace std::chrono;
        assert(open); open = false;
        auto stop = high_resolution_clock::now();
        total_duration += duration_cast<nanoseconds>(stop - timestamp).count();
        occurrences++;
    }
    rt_measurement() : total_duration(0), occurrences(0), open(false) {}
private:
    std::chrono::high_resolution_clock::time_point timestamp;
    std::uint64_t total_duration, occurrences;
    bool open;
};

/* Something to contain all runtime measurements, declared in main.cpp */
extern struct rt_analysis {
    rt_measurement setup;
    rt_measurement cache_hit;
    rt_measurement cache_miss;
    rt_measurement gc_select;
    rt_measurement gc_reclaim;
} rt_analysis;

