file(GLOB CONCORDCLIENT_SRC "src/*.cpp")

add_library(concordclient STATIC ${CONCORDCLIENT_SRC})
target_include_directories(concordclient PUBLIC include)
target_link_libraries(concordclient PRIVATE
  bftclient_new # temporary until client pool is available
  thin_replica_client
)
