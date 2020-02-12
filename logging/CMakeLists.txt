if (USE_CONAN)
  add_library(logging INTERFACE)
  target_include_directories(logging INTERFACE include/)

  #######################################################################################################################
  # Default CONCORD_LOGGER_NAME to "concord"

  if(NOT CONCORD_LOGGER_NAME)
      set(CONCORD_LOGGER_NAME "concord" CACHE STRING "Set concord logger name" FORCE)
  endif()

  #######################################################################################################################
  #export all the way up to the top level directory
  if(USE_LOG4CPP)
    find_package(log4cplus REQUIRED)
  endif()

  set(current_dir ${CMAKE_CURRENT_SOURCE_DIR})
  while(NOT current_dir STREQUAL "")
    message(STATUS "setting logger compile definitions for [${current_dir}]")
    if(USE_LOG4CPP)
      set_property(DIRECTORY ${current_dir} APPEND PROPERTY LINK_LIBRARIES ${LOG4CPLUS_LIBRARY})
      set_property(DIRECTORY ${current_dir} APPEND PROPERTY INCLUDE_DIRECTORIES ${LOG4CPLUS_INCLUDE_DIRS})
      set_property(DIRECTORY ${current_dir} APPEND PROPERTY COMPILE_DEFINITIONS USE_LOG4CPP)
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
else()
  add_library(logging INTERFACE)
  target_include_directories(logging INTERFACE include/)

  #######################################################################################################################
  #  Find log4cplus TODO [TK] drop when use conan
  #  Once done this will define
  #  LOG4CPLUS_FOUND        - System has log4cplus
  #  LOG4CPLUS_INCLUDE_DIRS - The log4cplus include directories
  #  LOG4CPLUS_LIBRARIES    - The libraries needed to use log4cplus
  #  LOG4CPLUS_DEFINITIONS  - Compiler switches required for using log4cplus

  if(USE_LOG4CPP)
    message(STATUS "USE_LOG4CPP")
    # First try pkg-config piggybacking
    find_package(PkgConfig)
    if (PKG_CONFIG_FOUND)
      message(STATUS "Trying to find log4cplus using PkgConfig")
      pkg_check_modules(PC_LOG4CPLUS log4cplus)
      if (PC_LOG4CPLUS_FOUND)
        if (NOT PC_LOG4CPLUS_LINK_LIBRARIES) # for older cmake versions
          set ( PC_LOG4CPLUS_LINK_LIBRARIES "${PC_LOG4CPLUS_LIBDIR}/lib${PC_LOG4CPLUS_LIBRARIES}.so")
        endif()
        set(LOG4CPLUS_LIBRARIES    ${PC_LOG4CPLUS_LINK_LIBRARIES})
        set(LOG4CPLUS_INCLUDE_DIRS ${PC_LOG4CPLUS_INCLUDE_DIRS}  )
      endif()
    endif()

    if (NOT PC_LOG4CPLUS_FOUND)
      message(STATUS "Trying to find log4cplus without PkgConfig")
      find_path(LOG4CPLUS_INCLUDE_DIRS log4cplus/config.hxx
              HINTS ${CMAKE_INSTALL_PREFIX}
              PATH_SUFFIXES include)

      find_library(LOG4CPLUS_LIBRARIES NAMES log4cplus
              HINTS ${CMAKE_INSTALL_PREFIX}
              PATH_SUFFIXES lib)

      include(FindPackageHandleStandardArgs)
      find_package_handle_standard_args(LOG4CPLUS DEFAULT_MSG LOG4CPLUS_LIBRARIES LOG4CPLUS_INCLUDE_DIRS)
    endif()
  endif()
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
      set_property(DIRECTORY ${current_dir} APPEND PROPERTY COMPILE_DEFINITIONS USE_LOG4CPP)
      set_property(DIRECTORY ${current_dir} APPEND PROPERTY LINK_LIBRARIES      ${LOG4CPLUS_LIBRARIES})
      set_property(DIRECTORY ${current_dir} APPEND PROPERTY INCLUDE_DIRECTORIES ${LOG4CPLUS_INCLUDE_DIRS})
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
endif()
