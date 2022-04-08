project("Thin Replica Client")

# Require C++17.
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
string(APPEND CMAKE_CXX_FLAGS " -pedantic") # Mostly for -Werror=vla
string(APPEND CMAKE_CXX_FLAGS " -march=x86-64") # Generate code specific for x68-64 architecture
string(APPEND CMAKE_CXX_FLAGS " -mtune=generic") # Generate code optimized for the most common processors

# CMake helper modules
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}/../cmake")

# Build the Thin Replica Client library
include_directories(${CMAKE_CURRENT_BINARY_DIR})
add_library(thin_replica_client_lib STATIC
  "src/thin_replica_client.cpp"
  "src/replica_state_snapshot_client.cpp"
  "src/trace_contexts.cpp"
  "src/grpc_connection.cpp"
  "src/trc_hash.cpp")
target_include_directories(thin_replica_client_lib PUBLIC include)
target_include_directories(thin_replica_client_lib PRIVATE
  "${secretsmanager_SOURCE_DIR}/include")
target_link_libraries(thin_replica_client_lib
  opentracing
  util
  thin-replica-proto
  replica-state-snapshot-proto
  concordclient-event-api
  concordclient-health
)

# Unit tests
if (BUILD_TESTING)
  add_subdirectory(test)
endif()
