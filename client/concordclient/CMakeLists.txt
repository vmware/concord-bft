add_library(concordclient STATIC "src/concord_client.cpp")
target_include_directories(concordclient PUBLIC include)
target_link_libraries(concordclient
PUBLIC
  bftclient_new
  concordclient-event-api
  util
PRIVATE
  concord_client_pool
  thin_replica_client_lib
)

find_package(Protobuf REQUIRED)
add_library(concordclient-event-api STATIC "src/event_update_queue.cpp")
target_include_directories(concordclient-event-api PUBLIC include)
target_link_libraries(concordclient-event-api PRIVATE
  protobuf::libprotobuf # Needed for the timestamp in EventGroup
  util
)

if (BUILD_TESTING)
  add_subdirectory(test)
endif()
