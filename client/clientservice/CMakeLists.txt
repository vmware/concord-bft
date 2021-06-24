add_subdirectory("proto")

find_package(GRPC REQUIRED)

file(GLOB CLIENTSERVICE_SRC "src/*.cpp")

add_executable(clientservice ${CLIENTSERVICE_SRC})
target_link_libraries(clientservice PRIVATE
  clientservice-proto
  concordclient
  gRPC::grpc++
  gRPC::grpc++_reflection
  logging
)
