add_library(concordclient "src/concord_client.cpp")
target_include_directories(concordclient PUBLIC include)
# TODO: Mark libraries as PRIVATE once the interface is selfcontained

add_subdirectory("cmf")

target_link_libraries(concordclient PUBLIC
  thin_replica_client_lib
  concord_client_request
  concord_client_pool
  concordclient-event-api
  util
)

find_package(Protobuf REQUIRED)
add_library(concordclient-event-api INTERFACE)
target_include_directories(concordclient-event-api INTERFACE include)
target_link_libraries(concordclient-event-api INTERFACE
  protobuf::libprotobuf # Needed for the timestamp in EventGroup
  util
)

if (BUILD_TESTING)
  add_subdirectory(test)
endif()
