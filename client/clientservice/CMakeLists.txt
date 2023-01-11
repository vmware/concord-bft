find_package(Protobuf REQUIRED)
find_package(gRPC REQUIRED)
find_package(Boost ${MIN_BOOST_VERSION} COMPONENTS program_options REQUIRED)
find_package(jaegertracing REQUIRED)
find_package(yaml-cpp ${YAML_CPP_VERSION} REQUIRED)
# All source files except for main.cpp are part of the library
file(GLOB CLIENTSERVICE_SRC "src/*.cpp")
list(FILTER CLIENTSERVICE_SRC EXCLUDE REGEX "src/main.cpp")

add_library(clientservice-lib STATIC ${CLIENTSERVICE_SRC})
target_include_directories(clientservice-lib PUBLIC include ${Boost_INCLUDE_DIR})
target_link_libraries(clientservice-lib PUBLIC
  clientservice-proto
  concordclient
  gRPC::grpc++
  gRPC::grpc++_reflection
  yaml-cpp
  secret_retriever
)

add_executable(clientservice "src/main.cpp")
target_link_libraries(clientservice PRIVATE
  clientservice-lib
  Boost::program_options
  jaegertracing
  thrift
  util_prometheus
)

if (BUILD_TESTING)
  add_subdirectory(test)
endif()
