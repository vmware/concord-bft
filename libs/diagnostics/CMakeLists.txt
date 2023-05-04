cmake_minimum_required (VERSION 3.2)
project(libdiagnostics VERSION 0.1.0.0 LANGUAGES CXX)

add_library(diagnostics src/status_handlers.cpp src/performance_handler.cpp)

set_property(TARGET diagnostics PROPERTY POSITION_INDEPENDENT_CODE ON)
if(BUILD_CONFIG_GEN_TOOL_FOR_MAC)
    target_include_directories(diagnostics PUBLIC /usr/local/include/ ../ ../util/include/)
endif()

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
