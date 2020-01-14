# pthread dependency
find_package(Threads REQUIRED)

add_library(util STATIC
    src/Metrics.cpp
    src/MetricsServer.cpp
    src/SimpleThreadPool.cpp
    src/histogram.cpp
    src/status.cpp
    src/sliver.cpp
    src/hex_tools.cpp)

target_link_libraries(util PUBLIC Threads::Threads hdr_histogram)
target_include_directories(util PUBLIC include)

if (BUILD_TESTING)
    add_subdirectory(pyclient)
    add_subdirectory(test)
endif()

set_property(DIRECTORY .. APPEND PROPERTY INCLUDE_DIRECTORIES
    ${CMAKE_CURRENT_SOURCE_DIR}/include/)
