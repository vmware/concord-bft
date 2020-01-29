#include <iostream>
#include "hdr_histogram.h"

int main() {
    struct hdr_histogram* histogram;

    // Initialise the histogram
    hdr_init(
        1,  // Minimum value
        INT64_C(3600000000),  // Maximum value
        3,  // Number of significant figures
        &histogram);  // Pointer to initialise

    // Record value
    hdr_record_value(
        histogram,  // Histogram to record to
        100);  // Value to record

    // Record value n times
    hdr_record_values(
        histogram,  // Histogram to record to
        1000,  // Value to record
        10);  // Record value 10 times

    // Record value with correction for co-ordinated omission.
    hdr_record_corrected_value(
        histogram,  // Histogram to record to
        10000,  // Value to record
        1000);  // Record with expected interval of 1000.

    // Print out the values of the histogram
    hdr_percentiles_print(
        histogram,
        stdout,  // File to write to
        5,  // Granularity of printed values
        1.0,  // Multiplier for results
        CLASSIC);  // Format CLASSIC/CSV supported.
}