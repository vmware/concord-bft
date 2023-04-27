option(CPPCHECK "Perform cppcheck" OFF)

if(CPPCHECK)
    set(BUILD_TESTING OFF) 
    set(BUILD_UTT OFF)
    set(USE_LOG4CPP OFF)	
    message(STATUS "CPPCHECK is ON")
    find_program(cppcheck cppcheck HINTS "/usr/local/bin/cppcheck" REQUIRED)
    message(STATUS "cppcheck ${cppcheck}")
    # Create <cppcheck> work folder for whole program analysis, for faster analysis and to store some useful debug information
    # Add cppcheck work folder and reports folder for cppcheck output.
	set(CPPCHECK_BUILD_DIR ${PROJECT_BINARY_DIR}/cppcheck)
	file(MAKE_DIRECTORY ${CPPCHECK_BUILD_DIR})
	message(STATUS "CPPCHECK directory: ${CPPCHECK_BUILD_DIR}")
    set(CMAKE_CXX_CPPCHECK
          "${cppcheck}"
          "--enable=warning"
          "--error-exitcode=1"
          "--inconclusive"
          "--inline-suppr"
          "--quiet"
          "--std=c++17"
          "--template={file}:{line}:({severity}):{id}: {message}"
          "--max-configs=1"
          "--library=boost.cfg"
          "--library=openssl.cfg"
          "--library=googletest"
          "--cppcheck-build-dir=${CPPCHECK_BUILD_DIR}"
          "--suppressions-list=${CMAKE_CURRENT_SOURCE_DIR}/.cppcheck/suppressions.txt"
          )
endif(CPPCHECK)
