cmake_minimum_required (VERSION 3.2)
project(libccron LANGUAGES CXX)

add_library(ccron
  src/ticks_generator.cpp
  src/periodic_action.cpp
  src/cron_table.cpp)

target_include_directories(ccron PUBLIC include)
target_link_libraries(ccron PUBLIC ccron_msgs corebft)

add_subdirectory(cmf)

if (BUILD_TESTING)
  add_subdirectory(test)
endif()
