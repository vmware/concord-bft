add_subdirectory(test)

add_library(concord_resources_monitor INTERFACE)

target_include_directories(concord_resources_monitor INTERFACE
  include
)

set_property(GLOBAL PROPERTY PERF_MANAGER_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/include)