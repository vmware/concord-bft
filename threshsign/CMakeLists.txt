cmake_minimum_required(VERSION 3.2)
project(threshsign VERSION 0.1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_EXTENSIONS OFF)

# Dependencies
find_package(Threads REQUIRED)

# Compiler flags
string(APPEND CXX_FLAGS " -Wall")
string(APPEND CXX_FLAGS " -Werror")
if(USE_LOG4CPP)
	#log4cplus needs pthread
	string(APPEND CXX_FLAGS " -pthread")
endif(USE_LOG4CPP)
string(APPEND CXX_FLAGS " -Wextra")
# TODO: Figure out right way to deal with -fstrict-overflow / -Wstrict-overflow related errors
string(APPEND CXX_FLAGS " -fno-strict-overflow")
string(APPEND CXX_FLAGS_DEBUG " -fstack-protector-all")
string(APPEND CXX_FLAGS_DEBUG " -D_FORTIFY_SOURCE=2")

# When you do 'a > b in 'C/C++, if a is unsigned and b is signed and equal to -1, C/C++
# actually casts b to unsigned (probably because casting unsigned to signed would require a bigger data type)
# Thus, 1 > -1 will evaluate to false because during the cast -1 will be set to to 2^32 - 1
#
# WARNING: For the love of god, do not remove this flag or you will regret it. Instead,
# just use signed types everywhere and cast your unsigned to signed when mixing unsigned
# variables with signed ones. See: http://soundsoftware.ac.uk/c-pitfall-unsigned
string(APPEND CXX_FLAGS " -Wconversion -Wsign-conversion")

if (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    string(APPEND CXX_FLAGS " -ferror-limit=3")
elseif ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
    string(APPEND CXX_FLAGS " -fmax-errors=3")
endif()

string(APPEND CMAKE_CXX_FLAGS " ${CXX_FLAGS}")
string(APPEND CMAKE_CXX_FLAGS_DEBUG " ${CXX_FLAGS_DEBUG}")
# When building with 'cmake -DCMAKE_BUILD_TYPE=Trace'
string(APPEND CMAKE_CXX_FLAGS_TRACE " -DTRACE")

enable_testing()

# Targets
if(USE_RELIC)
    if(BUILD_THIRDPARTY)
        add_dependencies(threshsign relic)
    else()
        find_library(RELIC_STATIC_LIBRARY NAMES "librelic_s.a")
        find_library(GMP_STATIC_LIBRARY NAMES "libgmp.a")
    endif()

    # Helper functions

    # Relic is a cryptographic "meta-toolkit" which can be used with different
    # concrete implementations. The user has to make sure that the latter is
    # available at compile-/runtime.
    function(link_with_relic_library targetName)
        target_link_libraries(${targetName}
                PRIVATE
                ${RELIC_STATIC_LIBRARY}
                ${GMP_STATIC_LIBRARY}
                )
    endfunction()

    # Tests and benchmarks that make use of Relic all need the same boilerplate setup
    function(add_relic_executable appName appSrc appDir)
        add_executable(${appName} ${appSrc} $<TARGET_OBJECTS:bench> $<TARGET_OBJECTS:bls_relic>)
        target_include_directories(${appName} PRIVATE
                ${threshsign_SOURCE_DIR}/src
                ${threshsign_SOURCE_DIR}/include
                ${RELIC_INCLUDE_DIRS})
        target_link_libraries(${appName} PRIVATE mainapp relic_mainapp threshsign)
        link_with_relic_library(${appName})

        set_target_properties(${appName} PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${appDir})
    endfunction()
endif()

function(add_threshsign_executable appName appSrc appDir)
    add_executable(${appName} ${appSrc})
    target_include_directories(${appName} PRIVATE ${threshsign_SOURCE_DIR}/src)
    target_link_libraries(${appName} PRIVATE mainapp)
    target_link_libraries(${appName} PRIVATE threshsign)
    if(USE_RELIC)
        link_with_relic_library(${appName})
    endif()
    set_target_properties(${appName} PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${appDir})
endfunction()

add_subdirectory(src)

if(BUILD_TESTING)
    if(USE_RELIC)
        add_subdirectory(bench)
    endif()
    add_subdirectory(test)
endif()
