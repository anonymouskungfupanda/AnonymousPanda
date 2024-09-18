#include "fio_trace_parser.hpp"

#ifndef STREAMLINED_TRACE_INPUT

fio_trace_parser::fio_trace_parser(std::vector<std::ifstream> &trace_files) {
    // Should only have one file if using fio traces
    assert(trace_files.size() == 1);
    file = &trace_files[0];
}

std::uint64_t fio_trace_parser::get_total_lines() {

    uint64_t total_lines = 0;
    for (std::string line; std::getline(*file, line);)
        total_lines++;

    file->clear();
    file->seekg(0, std::ios::beg);

    return total_lines;
}

std::tuple<bool, io_info> fio_trace_parser::get() {

    for (std::string line; std::getline(*file, line);) {
        std::stringstream line_stream(line);

        std::string part;
        std::vector<std::string> parts;

        while(std::getline(line_stream, part, ' '))
            parts.push_back(part);

        if (parts.size() == 4 && (parts[1] == "read" || parts[1] == "write")) {

            // All I/O in the fio trace must be in 4k increments
            assert(parts[3] == "4096");

            unsigned long long offset = std::atoll(parts[2].c_str());
            offset >>= 9;

            //std::cout << offset << std::endl;

            io_info io;
            io.is_read = parts[1] == "read";
            io.size_sectors = 8;
            io.sector = offset;

            //std::cout << io.is_read << " " << io.sector << " " << io.size_sectors << std::endl;

            return std::make_tuple(true, io);
        }          
    }

    return { false, { 0 } };
}

#else

std::tuple<bool, io_info> fio_trace_parser::get() {

    for (std::string line; std::getline(std::cin, line);) {
        std::stringstream line_stream(line);

        std::string part;
        std::vector<std::string> parts;

        while(std::getline(line_stream, part, ' '))
            parts.push_back(part);

        if (parts.size() == 4 && (parts[1] == "read" || parts[1] == "write")) {

            // All I/O in the fio trace must be in 4k increments
            assert(parts[3] == "4096");

            unsigned long long offset = std::atoll(parts[2].c_str());
            offset >>= 9;

            //std::cout << offset << std::endl;

            io_info io;
            io.is_read = parts[1] == "read";
            io.size_sectors = 8;
            io.sector = offset;

            //std::cout << io.is_read << " " << io.sector << " " << io.size_sectors << std::endl;

            return { true, io };
        }

        if (parts.size() == 2 && parts[1] == "close")
            return { false, { 0 } };
    }

    return { false, { 0 } };
}

#endif

