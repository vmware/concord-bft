cmake_minimum_required (VERSION 3.2)
project(libdiagnostics VERSION 0.1.0.0 LANGUAGES CXX)

add_library(diagnostics src/status_handlers.cpp src/performance_handler.cpp)

set_property(TARGET diagnostics PROPERTY POSITION_INDEPENDENT_CODE ON)
target_include_directories(diagnostics PUBLIC include)

if (BUILD_TESTING)
    add_subdirectory(test)
endif()
