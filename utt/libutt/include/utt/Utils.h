#pragma once

#include <vector>
#include <ostream>

/**
 * Returns k unique random numbers in the range [0, n-1] as a vector
 */
std::vector<size_t> random_subset(size_t k, size_t n);

/**
 * Originally from: https://stackoverflow.com/questions/669438/how-to-get-memory-usage-at-runtime-using-c
 *
 * process_mem_usage(double &, double &) - takes two doubles by reference,
 * attempts to read the system-dependent data for a process' virtual memory
 * size and resident set size, and return the results in bytes.
 *
 * On failure, returns 0, 0;
 */
void printMemUsage(const char * headerMessage = nullptr);

void getMemUsage(size_t& vm_usage, size_t& resident_set);
