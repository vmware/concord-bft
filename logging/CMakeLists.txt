project(logging  LANGUAGES CXX)


add_library(logging STATIC src/Logger.cpp )
target_include_directories(logging PUBLIC include/)
set_property(TARGET logging PROPERTY POSITION_INDEPENDENT_CODE ON)

if(USE_LOG4CPP)
    message(STATUS "USE_LOG4CPP")
    target_sources(logging PRIVATE src/Logging4cplus.cpp)

    find_path(LOG4CPLUS_INCLUDE_DIRS log4cplus/config.hxx
              HINTS ${CMAKE_INSTALL_PREFIX}	PATH_SUFFIXES include)
    find_library(LOG4CPLUS_STATIC_LIBRARY NAMES "liblog4cplus.a")
    message(STATUS "LOG4CPP include dir ${LOG4CPLUS_INCLUDE_DIRS}")
    message(STATUS "LOG4CPP static lib  ${LOG4CPLUS_STATIC_LIBRARY}")
    target_compile_definitions(logging PUBLIC USE_LOG4CPP)
    target_include_directories(logging PUBLIC ${LOG4CPLUS_INCLUDE_DIRS})
    target_link_libraries(logging PUBLIC ${LOG4CPLUS_STATIC_LIBRARY})
else(USE_LOG4CPP)
	target_sources(logging PRIVATE src/Logging.cpp)
endif(USE_LOG4CPP)


set_property(DIRECTORY ${CMAKE_SOURCE_DIR} APPEND PROPERTY LINK_LIBRARIES logging)
set_property(DIRECTORY .. APPEND PROPERTY LINK_LIBRARIES logging) 
