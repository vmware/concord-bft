add_subdirectory("proto")

find_package(GRPC REQUIRED)
find_package(Boost ${MIN_BOOST_VERSION} COMPONENTS program_options REQUIRED)

file(GLOB CLIENTSERVICE_SRC "src/*.cpp")

add_executable(clientservice ${CLIENTSERVICE_SRC})
target_link_libraries(clientservice PRIVATE
  Boost::program_options
  clientservice-proto
  concordclient
  gRPC::grpc++
  gRPC::grpc++_reflection
  logging
  yaml-cpp
  util
)
