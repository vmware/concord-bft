# Clang flags and options
string(APPEND CMAKE_CXX_FLAGS " -pedantic") # Mostly for -Werror=vla
string(APPEND CMAKE_CXX_FLAGS " -mtune=generic") # Generate code optimized for the most common processors
string(APPEND CMAKE_EXE_LINKER_FLAGS "-pie -Wl,-z,relro,-z,now")
string(APPEND CMAKE_CXX_FLAGS_RELEASE " -D_FORTIFY_SOURCE=2")

string(APPEND CMAKE_CXX_FLAGS_DEBUG " -fstack-protector-all")

string(APPEND CMAKE_CXX_FLAGS " -ferror-limit=3")

# Clang options for specific architecture
EXECUTE_PROCESS( COMMAND uname -m COMMAND tr -d '\n' OUTPUT_VARIABLE ARCHITECTURE )
message(STATUS "arch - ${ARCHITECTURE}")
if(ARCHITECTURE STREQUAL "x86_64")
	string(APPEND CMAKE_CXX_FLAGS " -march=x86-64")
elseif(ARCHITECTURE STREQUAL "aarch64")
	string(APPEND CMAKE_CXX_FLAGS " -march=native") # pick the architecture of the host system, e.g for M1
endif()

if( CODE_INSTRUMENTATION STREQUAL "CODECOVERAGE" )
  message( "Concord is being built for CODECOVERAGE.")
  string(APPEND CMAKE_CXX_FLAGS " -fprofile-instr-generate -fcoverage-mapping")
  string(APPEND CMAKE_EXE_LINKER_FLAGS " -fprofile-instr-generate ")
  set(BUILD_TESTING off )
elseif( CODE_INSTRUMENTATION STREQUAL "ADDRESS_SANITIZER" )
  message( "Concord is being built for ADDRESS_SANITIZER.")
  string(APPEND CMAKE_CXX_FLAGS "  -fno-omit-frame-pointer -fsanitize=leak -fsanitize=address")
  add_compile_definitions(RUN_WITH_LEAKCHECK=1)
  string(APPEND CMAKE_EXE_LINKER_FLAGS " -fsanitize=address ")
  set(BUILD_TESTING off )
elseif( CODE_INSTRUMENTATION STREQUAL "LEAK_SANITIZER" )
  message( "Concord is being built for LEAK_SANITIZER.")
  string(APPEND CMAKE_CXX_FLAGS "  -fno-omit-frame-pointer -fsanitize=leak")
  add_compile_definitions(RUN_WITH_LEAKCHECK=1)
  set(BUILD_TESTING off )
elseif( CODE_INSTRUMENTATION STREQUAL "THREAD_SANITIZER" )
  message( "Concord is being built for THREAD_SANITIZER.")
  string(APPEND CMAKE_CXX_FLAGS "  -fsanitize=thread")
  string(APPEND CMAKE_EXE_LINKER_FLAGS " -fsanitize=thread ")
  set(BUILD_TESTING off )
else()
  message( "Concord is being built without Clang instrumentation.")
endif()

#
# Code Quality (static, dynamic, coverage) Analysers
#
if(CODECOVERAGE)
    string(APPEND CMAKE_CXX_FLAGS " -fprofile-instr-generate -fcoverage-mapping")
    string(APPEND CMAKE_EXE_LINKER_FLAGS " -fprofile-instr-generate")
    message( "-- Building with llvm Code Coverage Tools")
endif()
