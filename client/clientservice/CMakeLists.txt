find_package(GRPC REQUIRED)
find_package(Boost ${MIN_BOOST_VERSION} COMPONENTS program_options REQUIRED)

# All source files except for main.cpp are part of the library
file(GLOB CLIENTSERVICE_SRC "src/*.cpp")
list(FILTER CLIENTSERVICE_SRC EXCLUDE REGEX "src/main.cpp")

add_library(clientservice-lib ${CLIENTSERVICE_SRC})
target_include_directories(clientservice-lib PUBLIC include)
target_link_libraries(clientservice-lib PUBLIC
  Boost::program_options
  clientservice-proto
  concordclient
  gRPC::grpc++
  gRPC::grpc++_reflection
  logging
  yaml-cpp
  util
)

add_executable(clientservice "src/main.cpp")
target_link_libraries(clientservice PRIVATE
  clientservice-lib
)

if (BUILD_TESTING)
  add_subdirectory(test)
endif()
