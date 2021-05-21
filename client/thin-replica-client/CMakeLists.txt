project("Thin Replica Client")

# CMake helper modules
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}/../cmake")

find_package(OpenSSL REQUIRED)

# Build the Thin Replica Client library
include_directories(${CMAKE_CURRENT_BINARY_DIR})
add_library(thin_replica_client STATIC
  "src/thin_replica_client.cpp"
  "src/trace_contexts.cpp"
  "src/trs_connection.cpp"
  "src/crypto_utils.cpp"
  "src/trc_hash.cpp")
target_include_directories(thin_replica_client PUBLIC include)
target_compile_definitions(thin_replica_client PUBLIC USE_LOG4CPP)
target_include_directories(thin_replica_client PRIVATE
  "${secretsmanager_SOURCE_DIR}/include")
target_link_libraries(thin_replica_client
  log4cplus
  opentracing
  util
  cryptopp
  thin-replica-proto
)

install(TARGETS thin_replica_client DESTINATION lib)
install(FILES include/client/thin-replica-client/thin_replica_client.hpp include/client/thin-replica-client/health_status.hpp include/client/thin-replica-client/update.hpp DESTINATION "include/thin_replica_client")

# Unit tests
if (BUILD_TESTING)
    add_subdirectory(test)
endif()
