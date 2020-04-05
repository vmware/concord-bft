cmake_minimum_required (VERSION 3.2)
project(libdiagnostics VERSION 0.1.0.0 LANGUAGES CXX)

add_library(diagnostics INTERFACE)
target_link_libraries(diagnostics INTERFACE)
target_include_directories(diagnostics INTERFACE include)

if (BUILD_TESTING)
    add_subdirectory(test)
endif()
