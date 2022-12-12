cmake_minimum_required(VERSION 3.24)
include(FetchContent)

if(USE_LOG4CPP)

# passing though CMAKE_ARGS doesn't work
string(APPEND CMAKE_CXX_FLAGS " -Wno-unused-function -Wno-unused-const-variable")
set(LOG4CPLUS_ENABLE_DECORATED_LIBRARY_NAME OFF CACHE BOOL "no postfix (S) for liblog4cplus.a" FORCE)
set(LOG4CPLUS_BUILD_LOGGINGSERVER OFF CACHE BOOL "don't build logging server" FORCE)
set(LOG4CPLUS_BUILD_TESTING OFF CACHE BOOL "don't vuild testeing" FORCE)

FetchContent_Declare(log4cplus
                    GIT_REPOSITORY "https://github.com/log4cplus/log4cplus.git"
                    GIT_TAG "REL_2_0_6"
                    GIT_SUBMODULES_RECURSE TRUE
                    GIT_SHALLOW TRUE
                    GIT_PROGRESS TRUE
                    OVERRIDE_FIND_PACKAGE
                    )

FetchContent_MakeAvailable(log4cplus)

endif(USE_LOG4CPP)
