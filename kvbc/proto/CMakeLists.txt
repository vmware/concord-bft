find_package(Protobuf REQUIRED)

protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${CMAKE_CURRENT_BINARY_DIR}
  concord_kvbc.proto
)
message(STATUS "Concord KVBC protobuf generated - see " ${CMAKE_CURRENT_BINARY_DIR})

add_library(concord-kvbc-proto STATIC ${PROTO_SRCS})
target_link_libraries(concord-kvbc-proto protobuf::libprotobuf)
target_include_directories(concord-kvbc-proto PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
