#pragma once

#include <tuple>
#include <string>
#include <vector>
#include <cassert>
#include <fstream>
#include <sstream>
#include <iostream>

#include "io.hpp"

/*
 * TODO: This is the macro which needs to be defined through the compiler flags in the Makefile
 */
// #define STREAMLINED_TRACE_INPUT

class fio_trace_parser
{
    std::ifstream *file;

public:

#ifndef STREAMLINED_TRACE_INPUT
    fio_trace_parser(std::vector<std::ifstream> &trace_files);
    std::uint64_t get_total_lines();
#endif

    std::tuple<bool, io_info> get();
};
