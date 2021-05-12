add_subdirectory("proto")

file(GLOB CLIENTSERVICE_SRC "src/*.cpp")

add_executable(clientservice ${CLIENTSERVICE_SRC})
target_link_libraries(clientservice PRIVATE
  clientservice-proto
)
