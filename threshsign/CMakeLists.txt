cmake_minimum_required (VERSION 3.2)
# project(<name> VERSION <ver> LANGUAGES CXX)
project(threshsign VERSION 0.1.0.0 LANGUAGES CXX)

find_package(Threads REQUIRED)

#
# C++ options
#   TODO: change to set_target_properties?
#   https://crascit.com/2015/03/28/enabling-cxx11-in-cmake/
#
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

#
# Compiler flags
#

# When you do 'a > b in 'C/C++, if a is unsigned and b is signed and equal to -1, C/C++
# actually casts b to unsigned (probably because casting unsigned to signed would require a bigger data type)
# Thus, 1 > -1 will evaluate to false because during the cast -1 will be set to to 2^32 - 1
#
# WARNING: For the love of god, do not remove this flag or you will regret it. Instead,
# just use signed types everywhere and cast your unsigned to signed when mixing unsigned
# variables with signed ones. See: http://soundsoftware.ac.uk/c-pitfall-unsigned
set(CXX_FLAGS_INTEGER_CORRECTNESS "-Wconversion -Wsign-conversion")

string(APPEND CXX_FLAGS " ${CXX_FLAGS_INTEGER_CORRECTNESS}")
# TODO: use target_compile_features instead:
#   https://cmake.org/cmake/help/v3.1/command/target_compile_features.html#command:target_compile_features
#   https://cmake.org/cmake/help/v3.1/prop_gbl/CMAKE_CXX_KNOWN_FEATURES.html#prop_gbl:CMAKE_CXX_KNOWN_FEATURES
string(APPEND CXX_FLAGS " -Wall")
string(APPEND CXX_FLAGS " -Werror")
string(APPEND CXX_FLAGS " -Wextra")


# TODO: Figure out right way to deal with -fstrict-overflow / -Wstrict-overflow related errors
string(APPEND CXX_FLAGS " -fno-strict-overflow")
string(APPEND CXX_FLAGS_DEBUG " -D_FORTIFY_SOURCE=2")

# GNU and Clang-specific flags
string(APPEND CMAKE_CXX_FLAGS " ${CXX_FLAGS}")
string(APPEND CMAKE_CXX_FLAGS_DEBUG " ${CXX_FLAGS_DEBUG}")

# using Clang
if (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    # Clang-specific options
    string(APPEND CMAKE_CXX_FLAGS " -ferror-limit=3")
    string(APPEND CMAKE_CXX_FLAGS_DEBUG " -fstack-protector-all")

    # using GCC
elseif ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
    # GCC-specific options
    string(APPEND CMAKE_CXX_FLAGS " -fmax-errors=3")
    string(APPEND CMAKE_CXX_FLAGS_DEBUG " -fstack-protector-all")

# TODO: using Intel C++
elseif ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Intel")

# TODO: using Visual Studio C++
elseif ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "MSVC")

endif()

#
# Testing flags
#
enable_testing()

# When building with 'cmake -DCMAKE_BUILD_TYPE=Trace'
string(APPEND CMAKE_CXX_FLAGS_TRACE " -DTRACE")

#
# Helper functions
#
function(link_with_relic_library targetName)
    target_link_libraries(${targetName}
        PUBLIC
        ${RELIC_STATIC_LIBRARY}
        ${GMP_STATIC_LIBRARY}
        ${CRYPTOPP_STATIC_LIBRARY})
endfunction()

function(add_relic_executable appName appSrc appDir)

    # Add compile target
    add_executable(${appName} ${appSrc} $<TARGET_OBJECTS:bench> $<TARGET_OBJECTS:bls_relic> $<TARGET_OBJECTS:common> )
    target_include_directories(${appName} PRIVATE
        ${threshsign_SOURCE_DIR}/src
        #${threshsign_SOURCE_DIR}/src/bls/relic
        #${threshsign_SOURCE_DIR}/src/bls/relic/async
        ${threshsign_SOURCE_DIR}/lib
        ${threshsign_SOURCE_DIR}/include)
    target_link_libraries(${appName} PRIVATE mainapp)
    target_link_libraries(${appName} PRIVATE relic_mainapp)
    target_link_libraries(${appName} PRIVATE Threads::Threads)
    link_with_relic_library(${appName})

    set_target_properties(${appName} PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${appDir})
endfunction()

function(add_threshsign_executable appName appSrc appDir)
    # Add compile target
    add_executable(${appName} ${appSrc} )
    # SbftKeygenApp depends on some RSA/Shoup internal functions/classes for generating primes
    target_include_directories(${appName} PRIVATE ${threshsign_SOURCE_DIR}/src ${threshsign_SOURCE_DIR}/lib ${threshsign_SOURCE_DIR}/include)
    target_link_libraries(${appName} PRIVATE mainapp)
    target_link_libraries(${appName} PUBLIC threshsign)
    link_with_relic_library(${appName})

    set_target_properties(${appName} PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${appDir})
endfunction()

#
# External dependencies
#
# NOTE: For this to work, you have to 'cmake', 'make', and 'sudo make install' these dependencies.
#
# TODO: Not sure if there's a lower effort way to do this. Too many complexities dealing with ExternalProject_Add
# having to do with configuring and installing the dependencies in the build directory and then building against it,
# which means you now have to reconfigure to install the dependencies in the system root like /usr/local. Better to
# simply count on having the dependency installed in the system.
# See: https://stackoverflow.com/questions/35934112/installing-an-externalproject-with-cmake
#  and
# http://cmake.3232098.n2.nabble.com/Trying-to-set-up-a-quot-superbuild-quot-with-external-project-and-install-it-td6353184.html
#
find_package(RELIC REQUIRED)
include_directories(${RELIC_INCLUDE_DIRS})
get_directory_property(hasParent PARENT_DIRECTORY)
if(hasParent)
    set_property(DIRECTORY .. APPEND PROPERTY INCLUDE_DIRECTORIES ${RELIC_INCLUDE_DIRS})
endif()
find_library(RELIC_STATIC_LIBRARY NAMES "librelic_s.a")

find_library(GMP_STATIC_LIBRARY NAMES "libgmp.a")

find_library(CRYPTOPP_STATIC_LIBRARY NAMES "libcryptopp.a")

#
# Targets
#

add_library(threshsign
    STATIC
    $<TARGET_OBJECTS:common>
    $<TARGET_OBJECTS:bls_relic>
)

target_include_directories(threshsign PUBLIC
    "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
    "$<INSTALL_INTERFACE:${INSTALL_CMAKE_DIR}/include>")

set_property(GLOBAL PROPERTY thresh_include_folder
        "${CMAKE_CURRENT_SOURCE_DIR}/include/threshsign")

# Only files in src/ include headers from lib/. Things that will depend on
# libthreshsign should not need code from lib/.
target_include_directories(threshsign PRIVATE lib)

target_link_libraries(threshsign PUBLIC Threads::Threads)
link_with_relic_library(threshsign)

#add_subdirectory(lib)
add_subdirectory(src)
add_subdirectory(bench)
add_subdirectory(test)
#add_subdirectory(app)

#
# Installation
# TODO: Add <Package>Config[Version].cmake files so this package can be easily imported?
# (See https://cmake.org/cmake/help/git-master/manual/cmake-packages.7.html#creating-packages)
#

# This creates the <Package>Config.cmake file and installs it
#install(TARGETS threshsign EXPORT threshsignConfig
#    ARCHIVE DESTINATION lib)
#install(EXPORT threshsignConfig DESTINATION lib/cmake/threshsign)
#
# This installs the static or (/and?) dynamic library
#install(TARGETS threshsign
#    ARCHIVE DESTINATION lib
#    LIBRARY DESTINATION lib
#)
#
#
#
# This installs the headers
# NOTE: Don't add / at the end. No slash means threshsign/ directory is created in the destination path
#install(DIRECTORY include/threshsign DESTINATION include)
