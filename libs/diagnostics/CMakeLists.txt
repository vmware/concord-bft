cmake_minimum_required (VERSION 3.2)
project(libdiagnostics VERSION 0.1.0.0 LANGUAGES CXX)

add_library(diagnostics src/status_handlers.cpp src/performance_handler.cpp)


find_path(HDR_HISTOGRAM_INCLUDE_DIR "hdr_interval_recorder.h" HINTS /usr/local/include/hdr REQUIRED)
find_library(HDR_HISTOGRAM_LIBRARY hdr_histogram_static HINTS /usr/local/lib REQUIRED)

set_property(TARGET diagnostics PROPERTY POSITION_INDEPENDENT_CODE ON)
target_include_directories(diagnostics PUBLIC ${HDR_HISTOGRAM_INCLUDE_DIR})
if(BUILD_CONFIG_GEN_TOOL_FOR_MAC)
    target_include_directories(diagnostics PUBLIC /usr/local/include/ ../ ../util/include/)
endif()
target_link_libraries(diagnostics ${HDR_HISTOGRAM_LIBRARY})

target_sources(diagnostics PUBLIC FILE_SET diagnostics_pub_hdrs
                                       TYPE HEADERS
                                       FILES 
                                       diagnostics_server.hpp
                                       diagnostics.hpp
                                       protocol.hpp
                                       status_handlers.hpp
                                       performance_handler.hpp)

install(TARGETS diagnostics FILE_SET diagnostics_pub_hdrs DESTINATION diagnostics)

if (BUILD_TESTING)
    add_subdirectory(test)
endif()
