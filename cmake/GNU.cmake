# GCC flags and options
message(STATUS "Using GCC configuration")
string(APPEND CMAKE_CXX_FLAGS " -pedantic") # Mostly for -Werror=vla
string(APPEND CMAKE_CXX_FLAGS " -mtune=generic") # Generate code optimized for the most common processors
string(APPEND CMAKE_EXE_LINKER_FLAGS " -pie -Wl,-z,relro,-z,now")
string(APPEND CMAKE_CXX_FLAGS " -Wall")
string(APPEND CMAKE_CXX_FLAGS " -Wbuiltin-macro-redefined")
string(APPEND CMAKE_CXX_FLAGS " -pedantic")
string(APPEND CMAKE_CXX_FLAGS " -Werror")
string(APPEND CMAKE_CXX_FLAGS " -fno-omit-frame-pointer")
# TODO: Figure out right way to deal with -fstrict-overflow / -Wstrict-overflow related errors
# string(APPEND CXX_FLAGS " -fno-strict-overflow")
# Prevents some buffer overflows: https://access.redhat.com/blogs/766093/posts/1976213
string(APPEND CMAKE_CXX_FLAGS_RELEASE " -D_FORTIFY_SOURCE=2")
string(APPEND CMAKE_CXX_FLAGS_DEBUG " -fstack-protector-all")
string(APPEND CMAKE_CXX_FLAGS " -fmax-errors=3")


# GCC options for specific architecture
EXECUTE_PROCESS( COMMAND uname -m COMMAND tr -d '\n' OUTPUT_VARIABLE ARCHITECTURE )
message(STATUS "arch - ${ARCHITECTURE}")
if(ARCHITECTURE STREQUAL "x86_64")
	string(APPEND CMAKE_CXX_FLAGS " -march=x86-64")
elseif(ARCHITECTURE STREQUAL "aarch64")
	string(APPEND CMAKE_CXX_FLAGS " -march=native") # pick the architecture of the host system, e.g for M1
endif()

# GCC code instrumentation options
if (CODE_INSTRUMENTATION)
	message(STATUS "Concord is being instrumented for ${CODE_INSTRUMENTATION}.")
	message(STATUS "Testing is enabled")
	set(BUILD_TESTING ON)
	if( CODE_INSTRUMENTATION STREQUAL "COVERAGE" )
	  string(APPEND CMAKE_CXX_FLAGS " -g -O0 --coverage")
	  string(APPEND CMAKE_EXE_LINKER_FLAGS " -g -O0 --coverage")
	elseif( CODE_INSTRUMENTATION STREQUAL "ADDRESS" )
	  string(APPEND CMAKE_CXX_FLAGS " -fsanitize=address")
	  string(APPEND CMAKE_EXE_LINKER_FLAGS " -fsanitize=address ")
	elseif( CODE_INSTRUMENTATION STREQUAL "LEAK" )
	  string(APPEND CMAKE_CXX_FLAGS " -fsanitize=leak")
	  add_compile_definitions(RUN_WITH_LEAKCHECK=1)
	elseif( CODE_INSTRUMENTATION STREQUAL "THREAD" )
	  string(APPEND CMAKE_CXX_FLAGS " -fsanitize=thread")
	  string(APPEND CMAKE_EXE_LINKER_FLAGS " -fsanitize=thread")
	else()
		message(FATAL_ERROR "Unknown instrumentation option: ${CODE_INSTRUMENTATION}")
	endif()
endif()

