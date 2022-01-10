add_subdirectory(test)

add_library(concord_resources_manager INTERFACE)

target_include_directories(concord_resources_manager INTERFACE
  include
)
