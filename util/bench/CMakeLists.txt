cmake_minimum_required(VERSION 3.2)

find_package(Boost ${MIN_BOOST_VERSION} COMPONENTS program_options REQUIRED)
add_executable(sign_verify_bench benchmarkSignVerify.cpp)

target_link_libraries(sign_verify_bench
                      util
                      Boost::program_options
                      stdc++fs)

target_include_directories(sign_verify_bench PUBLIC ../../thirdparty/include)
