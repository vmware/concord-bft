find_package(Boost ${MIN_BOOST_VERSION} COMPONENTS program_options REQUIRED)

add_executable(kvbcbench main.cpp)
target_link_libraries(kvbcbench PUBLIC
    kvbc
    util
    ${Boost_LIBRARIES}
)
