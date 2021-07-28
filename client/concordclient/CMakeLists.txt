add_library(concordclient "src/concord_client.cpp")
target_include_directories(concordclient PUBLIC include)
# TODO: Mark libraries as PRIVATE once the interface is selfcontained
target_link_libraries(concordclient PUBLIC
  thin_replica_client_lib
  concord_client_pool
  util
)
