#
# Targets
#

add_library(xassert
    src/XAssert.cpp
)

target_include_directories(xassert PUBLIC
    "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
    #"$<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}/include>"   # after I removed this, libxutils still didn't cmake without errors
    #"$<INSTALL_INTERFACE:${INSTALL_CMAKE_DIR}/include>"        # after I removed this too, libxutils cmake'd without errors
)

#
# Subdirectories
#
add_subdirectory(test)

#
# Installation
#

# TODO: Add <Package>Config[Version].cmake files so this package can be easily imported?
# (See https://cmake.org/cmake/help/git-master/manual/cmake-packages.7.html#creating-packages)

# This creates the <Package>Config.cmake file and installs it
install(TARGETS xassert EXPORT xassertConfig 
   ARCHIVE DESTINATION lib)
install(EXPORT xassertConfig DESTINATION lib/cmake/xassert)

# This installs the static or (/and?) dynamic library
install(TARGETS xassert 
    ARCHIVE DESTINATION lib
    LIBRARY DESTINATION lib
)

# This installs the headers
# WARNING: Don't add / at the end. No slash means xassert/ directory is created in the destination path
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/xassert DESTINATION include)
