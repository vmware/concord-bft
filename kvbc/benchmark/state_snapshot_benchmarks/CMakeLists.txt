find_package(Boost ${MIN_BOOST_VERSION} COMPONENTS program_options REQUIRED)

add_executable(hash_state_benchmark hash_state_benchmark.cpp)
target_link_libraries(hash_state_benchmark PUBLIC
    kvbc
    util
    ${Boost_LIBRARIES}
)
