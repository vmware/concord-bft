cmake_minimum_required(VERSION 3.24)

find_package(Boost ${MIN_BOOST_VERSION} COMPONENTS program_options REQUIRED)

FetchContent_Declare(picobench
                    GIT_REPOSITORY "https://github.com/iboB/picobench.git"
                    GIT_TAG "v2.01"
                    GIT_SHALLOW TRUE
                    GIT_PROGRESS TRUE
                    OVERRIDE_FIND_PACKAGE
                    )

FetchContent_MakeAvailable(picobench)

add_executable(sign_verify_bench benchmarkSignVerify.cpp)

target_link_libraries(sign_verify_bench
                      concord-crypto
                      Boost::program_options
                      picobench
                      )

