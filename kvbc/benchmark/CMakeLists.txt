find_package(benchmark)

if(benchmark_FOUND)
    add_executable(sparse_merkle_benchmark sparse_merkle_benchmark.cpp $<TARGET_OBJECTS:logging_dev>)
    target_link_libraries(sparse_merkle_benchmark PUBLIC
        benchmark
        util
        corebft
        kvbc
    )
endif()
