# Use Google Benchmark as a benchmarking library: https://github.com/google/benchmark
#
# Note: Benchmarks are not officially supported yet and are optional. Use QUIET to
# silence CMake in case Google Benchmark is not installed.
find_package(benchmark QUIET)

if(benchmark_FOUND)
    add_executable(sparse_merkle_benchmark sparse_merkle_benchmark.cpp )
    target_link_libraries(sparse_merkle_benchmark PUBLIC
        benchmark
        util
        concord-crypto
        corebft
        kvbc
    )

    if (BUILD_ROCKSDB_STORAGE)
    add_executable(categorization_benchmark categorization_benchmark.cpp )
    target_link_libraries(categorization_benchmark PUBLIC
        benchmark
        util
        corebft
        kvbc
    )
    endif (BUILD_ROCKSDB_STORAGE)
endif(benchmark_FOUND)

if (BUILD_TESTING)
    if (BUILD_ROCKSDB_STORAGE)
        add_subdirectory(kvbcbench)
        add_subdirectory(state_snapshot_benchmarks)
    endif (BUILD_ROCKSDB_STORAGE)
endif(BUILD_TESTING	)
