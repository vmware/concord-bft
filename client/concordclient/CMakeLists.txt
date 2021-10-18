add_library(concordclient "src/concord_client.cpp")
target_include_directories(concordclient PUBLIC include)
# TODO: Mark libraries as PRIVATE once the interface is selfcontained
target_link_libraries(concordclient PUBLIC
  thin_replica_client_lib
  concord_client_pool
  concordclient-event-api
  util
  secret_retriever
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
