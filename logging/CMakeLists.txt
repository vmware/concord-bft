add_library(logging INTERFACE)
target_include_directories(logging INTERFACE include/)

#default logger initialization for tests
add_library(logging_dev OBJECT src/Logger.cpp)
target_include_directories(logging_dev PUBLIC include/)

# optional log4cplus support
if(${USE_LOG4CPP})
  include (FindPackageHandleStandardArgs)
  set (CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}" CACHE PATH "CMake module path" FORCE )
  find_package ("Log4cplus")
  add_compile_definitions(USE_LOG4CPP)
  set_property(DIRECTORY .. APPEND PROPERTY LINK_LIBRARIES ${LOG4CPLUS_LIBRARIES})
  set_property(DIRECTORY .. APPEND PROPERTY INCLUDE_DIRECTORIES "${LOG4CPLUS_INCLUDES}")
  set_property(DIRECTORY .. APPEND PROPERTY COMPILE_DEFINITIONS USE_LOG4CPP) 
  target_include_directories(logging_dev PUBLIC ${LOG4CPLUS_INCLUDES})
endif()

# Default CONCORD_LOGGER_NAME to "concord"
if(NOT CONCORD_LOGGER_NAME)
    set(CONCORD_LOGGER_NAME "concord" CACHE STRING "Set concord logger name" FORCE)
endif()
target_compile_definitions(logging INTERFACE DEFAULT_LOGGER_NAME="${CONCORD_LOGGER_NAME}")
target_compile_definitions(logging_dev PUBLIC DEFAULT_LOGGER_NAME="${CONCORD_LOGGER_NAME}")

set_property(DIRECTORY .. APPEND PROPERTY COMPILE_DEFINITIONS DEFAULT_LOGGER_NAME="${CONCORD_LOGGER_NAME}") 
set_property(DIRECTORY .. APPEND PROPERTY INCLUDE_DIRECTORIES "$<TARGET_PROPERTY:logging,INTERFACE_INCLUDE_DIRECTORIES>")
set_property(DIRECTORY .. APPEND PROPERTY COMPILE_OPTIONS "-Wformat=2")
