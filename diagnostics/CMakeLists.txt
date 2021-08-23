cmake_minimum_required (VERSION 3.2)
project(libdiagnostics VERSION 0.1.0.0 LANGUAGES CXX)

add_library(diagnostics src/status_handlers.cpp src/performance_handler.cpp)

if(BUILD_THIRDPARTY)
    add_dependencies(diagnostics hdrhistogram)
else()
    find_path(HDR_HISTOGRAM_INCLUDE_DIR "hdr_interval_recorder.h" HINTS /usr/local/include/hdr REQUIRED)
    find_library(HDR_HISTOGRAM_LIBRARY hdr_histogram HINTS /usr/local/lib REQUIRED)
endif()

set_property(TARGET diagnostics PROPERTY POSITION_INDEPENDENT_CODE ON)
target_include_directories(diagnostics PUBLIC include ${HDR_HISTOGRAM_INCLUDE_DIR})
target_link_libraries(diagnostics ${HDR_HISTOGRAM_LIBRARY})

if (BUILD_TESTING)
    add_subdirectory(test)
endif()
