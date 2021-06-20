if(BUILD_SLOWDOWN)
    add_subdirectory(test)
endif()

set_property(GLOBAL PROPERTY PERF_MANAGER_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/include)