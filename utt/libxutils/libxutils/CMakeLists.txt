#
# Targets
#

add_library(xutils
    src/Log.cpp
    src/Timer.cpp
    src/Utils.cpp
)

target_link_libraries(xutils
    PUBLIC xassert
)

target_include_directories(xutils PUBLIC
    "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
    #"$<INSTALL_INTERFACE:${INSTALL_CMAKE_DIR}/include>"    # libbilinear fails cmake'ing when this is set in libxutils
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
install(TARGETS xutils EXPORT xutilsConfig 
   ARCHIVE DESTINATION lib)
install(EXPORT xutilsConfig DESTINATION lib/cmake/xutils)

# This installs the static or (/and?) dynamic library
install(TARGETS xutils 
    ARCHIVE DESTINATION lib
    LIBRARY DESTINATION lib
)

# This installs the headers
# WARNING: Don't add / at the end. No slash means libxutils/ directory is created in the destination path
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/xutils DESTINATION include)
