cmake_minimum_required (VERSION 3.2)
project(libdiagnostics VERSION 0.1.0.0 LANGUAGES CXX)

add_library(diagnostics src/status_handlers.cpp)
target_include_directories(diagnostics PUBLIC include)

if (BUILD_TESTING)
    add_subdirectory(test)
endif()
