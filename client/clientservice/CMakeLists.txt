find_package(GRPC REQUIRED)
find_package(Boost ${MIN_BOOST_VERSION} COMPONENTS program_options REQUIRED)
find_package(jaegertracing REQUIRED)

# All source files except for main.cpp are part of the library
file(GLOB CLIENTSERVICE_SRC "src/*.cpp")
list(FILTER CLIENTSERVICE_SRC EXCLUDE REGEX "src/main.cpp")

add_library(clientservice-lib ${CLIENTSERVICE_SRC})
target_include_directories(clientservice-lib PUBLIC include)
target_link_libraries(clientservice-lib PUBLIC
  clientservice-proto
  concordclient
  gRPC::grpc++
  gRPC::grpc++_reflection
  logging
  yaml-cpp
  secret_retriever
)

add_executable(clientservice "src/main.cpp")
target_link_libraries(clientservice PRIVATE
  Boost::program_options
  clientservice-lib
  jaegertracing
  logging
  thrift
  util_prometheus
)

if (BUILD_TESTING)
  add_subdirectory(test)
endif()
