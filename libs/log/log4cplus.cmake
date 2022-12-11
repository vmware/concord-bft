cmake_minimum_required(VERSION 3.24)
include(FetchContent)

if(USE_LOG4CPP)
string(APPEND CMAKE_CXX_FLAGS " -Wno-unused-function -Wno-unused-const-variable")
# log4cplus library
FetchContent_Declare(log4cplus
                    GIT_REPOSITORY "https://github.com/log4cplus/log4cplus.git"
                    GIT_TAG "REL_2_0_4"
                    GIT_SUBMODULES_RECURSE TRUE
                    GIT_SHALLOW TRUE
                    GIT_PROGRESS TRUE
                    CMAKE_ARGS  -DBUILD_SHARED_LIBS=OFF
                                -DLOG4CPLUS_BUILD_TESTING=OFF
                                -DCMAKE_INSTALL_PREFIX:PATH=${THIRDPARTY_INSTALL_DIR}
                                -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
                    OVERRIDE_FIND_PACKAGE
                    )

FetchContent_MakeAvailable(log4cplus)
endif(USE_LOG4CPP)
