cmake_minimum_required(VERSION 3.24)

find_package(Boost ${MIN_BOOST_VERSION} COMPONENTS program_options REQUIRED)

add_executable(threshsign_bench BenchmarkThreshsign.cpp)
target_link_libraries(threshsign_bench threshsign Boost::program_options picobench)
target_include_directories(threshsign_bench PUBLIC ../..)
