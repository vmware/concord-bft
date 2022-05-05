add_library(concordclient STATIC "src/concord_client.cpp")
target_include_directories(concordclient PUBLIC include)
target_link_libraries(concordclient
PUBLIC
  util
  bftclient_new
  concordclient-event-api
PRIVATE
  thin_replica_client_lib
  concord_client_pool
)

# Provide type definitions in a separate library
add_library(concordclient-interface INTERFACE)
target_include_directories(concordclient-interface INTERFACE include)
target_link_libraries(concordclient-interface INTERFACE bftclient_new-interface)

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
