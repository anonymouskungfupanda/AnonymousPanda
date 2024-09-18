#include <unordered_map>
#include <iostream>
#include <fstream>
#include <cstdint>
#include <memory>
#include <vector>
#include <string>

#include "rt_analysis.hpp"
#include "blktrace.hpp"
#include "dmcache.hpp"

/* Externally declared in rt_analysis.hpp */
struct rt_analysis rt_analysis;

/**
 * Parses command line arguments, populates the unordered_map with any command line arguments
 *      specified and returns a vector of the trace files.
 */
std::vector<std::string> parse_cmdline_args(int argc, char **argv, std::unordered_map<std::string, std::string> &arg_map) {

    std::vector<std::string> ret;

    for (int i = 1; i < argc; i++) {

        std::string arg_name(argv[i]);

        /* Is a command line argument, enter it in the map. If it already exists and the argument
         *      was provided twice for some reason, it will just be overwritten. */
        if (arg_name.at(0) == '-' && arg_name.at(1) == '-' && (i + 1) < argc) {

            /* Need to remove the '--' from each argument name */
            arg_name.erase(0, 2);

            /* Add to the arg_map and skip the next argument, since it is the parameter */
            arg_map[arg_name] = std::string(argv[i + 1]);
            i++;
            continue;
        }

        /* Current string is a trace file */
        ret.push_back(arg_name);
    }

    return ret;
}

static const char *usage_str = "Usage: <trace_files> <arguments>\n"
    "Arguments (take precedence over config file arguments and source code macros):\n"
    "\t[ --emulate_dmsetup              : < 0 or 1 > ]\n "
    "\t[ --zbd_cache                    : < 0 or 1 > ]\n "
    "\t[ --block_size                   : < Size in sectors > ]\n "
    "\t[ --base_size                    : < Size in sectors > ]\n "
    "\t[ --cache_size                   : < Size in blocks > ]\n "
    "\t[ --nr_zones                     : < Size in zones > ]\n "
    "\t[ --zone_size                    : < Size in sectors > ]\n "
    "\t[ --zone_cap                     : < Size in sectors > ]\n "
    "\t[ --dump_zbd_metadata            : < 0 or 1 > ]\n "
    "\t[ --dump_bitmaps                 : < 0 or 1 > ]\n "
    "\t[ --operation_logging            : < 0 or 1 > ]\n "
    "\t[ --use_fio_traces               : < 0 or 1 > ]\n "
    "\t[ --statistics_logging           : < 0 or 1 > ]\n "
    "\t[ --statistics_logging_frequency : < 1 or greater > ]\n"
    "\t[ --cache_admission_enabled      : < 0 or 1 > ]\n "
    "\t[ --gc_configuration             : < A string in the format: \"AA/BB-CC-DD\" >\n"
    "\t\tAA : GC_OP_PERCENTAGE\n"
    "\t\tBB : GC_OP_ACCEPTABLE_GARBAGE_PERCENTAGE\n"
    "\t\tCC : GC_OP_STOP_START_GAP_PERCENTAGE\n"
    "\t\tDD : GC_OP_START_BLOCK_GAP_PERCENTAGE ]\n"
    "\t[ --gc_policy                    : < relocation or eviction > ]\n";

int main(int argc, char **argv) {

    /* Stores command line arguments, excluding provided trace files */
    std::unordered_map<std::string, std::string> arg_map;

    if (argc < 2) {
        std::cout << usage_str << std::endl;
        return 1;
    }

    std::vector<std::string> trace_file_names = parse_cmdline_args(argc, argv, arg_map);
    std::vector<std::ifstream> trace_files;
    RT_START(setup);

    /* Opening blktrace log files */
    for (auto file_name : trace_file_names)
        trace_files.push_back(std::ifstream(file_name, std::ifstream::binary));

    /* The first file name is used as the trace name for automation logging */
    std::unique_ptr<dmcache> sim = std::make_unique<dmcache>(arg_map, trace_files, trace_file_names[0]);
    RT_STOP(setup);
    sim->run();

    RT_REPORT();
    return 0;
}
