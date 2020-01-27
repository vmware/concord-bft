add_library(logging INTERFACE)
target_include_directories(logging INTERFACE include/)

#######################################################################################################################
# Default CONCORD_LOGGER_NAME to "concord"

if(NOT CONCORD_LOGGER_NAME)
    set(CONCORD_LOGGER_NAME "concord" CACHE STRING "Set concord logger name" FORCE)
endif()

#######################################################################################################################
#export all the way up to the top level directory

set(current_dir ${CMAKE_CURRENT_SOURCE_DIR})
while(NOT current_dir STREQUAL "")
  message(STATUS "setting logger compile definitions for [${current_dir}]")
  if(USE_LOG4CPP)
    set_property(DIRECTORY ${current_dir} APPEND PROPERTY LINK_LIBRARIES      CONAN_PKG::log4cplus)
  endif()
  set_property(DIRECTORY ${current_dir} APPEND PROPERTY COMPILE_DEFINITIONS DEFAULT_LOGGER_NAME="${CONCORD_LOGGER_NAME}")
  set_property(DIRECTORY ${current_dir} APPEND PROPERTY INCLUDE_DIRECTORIES "$<TARGET_PROPERTY:logging,INTERFACE_INCLUDE_DIRECTORIES>")
  set_property(DIRECTORY ${current_dir} APPEND PROPERTY COMPILE_OPTIONS "-Wformat=2")
  set_property(DIRECTORY ${current_dir} APPEND PROPERTY COMPILE_OPTIONS "-Wno-deprecated") # TODO delete when upgrading to a newer version 
  get_property(current_dir DIRECTORY ${current_dir} PROPERTY PARENT_DIRECTORY)
endwhile()
#######################################################################################################################
#default logger initialization for tests

add_library(logging_dev OBJECT src/Logger.cpp)
#current version of log4cplus uses deprecated std::auto_ptr
target_compile_options(logging_dev PUBLIC -Wno-deprecated-declarations)
target_include_directories(logging_dev PUBLIC include/)
