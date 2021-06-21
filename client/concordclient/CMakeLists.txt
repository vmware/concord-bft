add_library(concordclient INTERFACE)
target_include_directories(concordclient INTERFACE include)
target_link_libraries(concordclient INTERFACE
  bftclient_new # temporary until client pool is available
  thin_replica_client_lib
)
