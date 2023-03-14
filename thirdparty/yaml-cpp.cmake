cmake_minimum_required(VERSION 3.24)

# yaml-cpp library
FetchContent_Declare(yaml-cpp
                    GIT_REPOSITORY "https://github.com/jbeder/yaml-cpp.git"
                    GIT_TAG "yaml-cpp-0.7.0"
                    GIT_SUBMODULES_RECURSE TRUE
                    GIT_SHALLOW TRUE
                    GIT_PROGRESS TRUE
                    PATCH_COMMAND git apply ${CMAKE_CURRENT_SOURCE_DIR}/thirdparty/yaml-cpp_cmakelists.patch || true
                    CMAKE_ARGS -DYAML_CPP_BUILD_TESTS=OFF
                               -DYAML_CPP_BUILD_TOOLS=OFF
                               -DYAML_BUILD_SHARED_LIBS=OFF
                               -DCMAKE_INSTALL_PREFIX:PATH=${THIRDPARTY_INSTALL_DIR}
                    OVERRIDE_FIND_PACKAGE
                    )

FetchContent_MakeAvailable(yaml-cpp)
