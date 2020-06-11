project(logging  LANGUAGES CXX)


add_library(logging STATIC src/Logger.cpp )
target_include_directories(logging PUBLIC include/)
set_property(TARGET logging PROPERTY POSITION_INDEPENDENT_CODE ON)

if(USE_LOG4CPP)
	message(STATUS "USE_LOG4CPP")
	target_sources(logging PRIVATE src/Logging4cplus.cpp)

    ###############################################################################################################
    #  Find log4cplus
    #  Once done this will define
    #  LOG4CPLUS_FOUND        - System has log4cplus
    #  LOG4CPLUS_INCLUDE_DIRS - The log4cplus include directories
    #  LOG4CPLUS_LIBRARY      - The libraries needed to use log4cplus
    #  LOG4CPLUS_DEFINITIONS  - Compiler switches required for using log4cplus

    # First try pkg-config piggybacking
    find_package(PkgConfig)
    if (PKG_CONFIG_FOUND)
        message(STATUS "Trying to find log4cplus using PkgConfig")
        pkg_check_modules(PC_LOG4CPLUS log4cplus)
        if (PC_LOG4CPLUS_FOUND)
            if (NOT PC_LOG4CPLUS_LINK_LIBRARIES) # for older cmake versions
                set ( PC_LOG4CPLUS_LINK_LIBRARIES "${PC_LOG4CPLUS_LIBDIR}/lib${PC_LOG4CPLUS_LIBRARIES}.so")
        endif()
        endif(PC_LOG4CPLUS_FOUND)
        set(LOG4CPLUS_LIBRARY    	${PC_LOG4CPLUS_LINK_LIBRARIES})
        set(LOG4CPLUS_INCLUDE_DIRS	${PC_LOG4CPLUS_INCLUDE_DIRS}  )
    endif(PKG_CONFIG_FOUND)

    if (NOT PC_LOG4CPLUS_FOUND)
        message(STATUS "Trying to find log4cplus without PkgConfig")
        find_path(LOG4CPLUS_INCLUDE_DIRS log4cplus/config.hxx
                  HINTS ${CMAKE_INSTALL_PREFIX}	PATH_SUFFIXES include)

        find_library(LOG4CPLUS_LIBRARY NAMES log4cplus
                     HINTS ${CMAKE_INSTALL_PREFIX}  PATH_SUFFIXES lib)

        include(FindPackageHandleStandardArgs)
        find_package_handle_standard_args(LOG4CPLUS DEFAULT_MSG LOG4CPLUS_LIBRARIES LOG4CPLUS_INCLUDE_DIRS)
    endif(NOT PC_LOG4CPLUS_FOUND)
	target_compile_definitions(logging PUBLIC USE_LOG4CPP)
	target_include_directories(logging PUBLIC ${LOG4CPLUS_INCLUDE_DIRS})
	target_link_libraries(logging PUBLIC ${LOG4CPLUS_LIBRARY})
else(USE_LOG4CPP)
	target_sources(logging PRIVATE src/Logging.cpp)
endif(USE_LOG4CPP)


set_property(DIRECTORY ${CMAKE_SOURCE_DIR} APPEND PROPERTY LINK_LIBRARIES logging)
set_property(DIRECTORY .. APPEND PROPERTY LINK_LIBRARIES logging) 
