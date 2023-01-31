cmake_minimum_required(VERSION 3.24)

find_package(Boost ${MIN_BOOST_VERSION} COMPONENTS program_options REQUIRED)

add_executable(sign_verify_bench benchmarkSignVerify.cpp)

target_link_libraries(sign_verify_bench
                      concord-crypto
                      Boost::program_options
                      picobench
                      )
