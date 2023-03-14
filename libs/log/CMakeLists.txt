project(logging LANGUAGES CXX)

add_library(logging STATIC src/logger.cpp )
target_include_directories(logging PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/../ PRIVATE .)

if(USE_LOG4CPP)
    message(STATUS "USE_LOG4CPP")
    include(FetchContent)
    set(FETCHCONTENT_QUIET FALSE)
    set(LOG4CPLUS_BUILD_TESTING OFF CACHE BOOL "dont buid log4cplus testing" FORCE)
    set(LOG4CPLUS_BUILD_LOGGINGSERVER OFF CACHE BOOL "dont buid log4cplus testing" FORCE)
    string(APPEND CMAKE_CXX_FLAGS " -Wno-unused-function -Wno-unused-const-variable")
    FetchContent_Declare(log4cplus
                         GIT_REPOSITORY    https://github.com/log4cplus/log4cplus.git
                         GIT_TAG           REL_2_0_6
                         GIT_PROGRESS      TRUE
                        )
    FetchContent_MakeAvailable(log4cplus)
    target_sources(logging PRIVATE src/logging4cplus.cpp)
    target_compile_definitions(logging PUBLIC USE_LOG4CPP)
    target_link_libraries(logging PUBLIC log4cplusS)
else(USE_LOG4CPP)
    target_sources(logging PRIVATE src/logging.cpp)
endif(USE_LOG4CPP)
