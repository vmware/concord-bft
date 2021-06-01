find_package(Protobuf REQUIRED)
find_package(GRPC REQUIRED)

include_directories(${GRPC_INCLUDE_DIR})

protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${CMAKE_CURRENT_BINARY_DIR}
  concord_kvbc.proto
)
grpc_generate_cpp(GRPC_SRCS GRPC_HDRS ${CMAKE_CURRENT_BINARY_DIR}
  concord_kvbc.proto
)
message(STATUS "Concord KVBC gRPC/protobuf generated - see " ${CMAKE_CURRENT_BINARY_DIR})

add_library(concord-kvbc-proto STATIC ${PROTO_SRCS} ${GRPC_SRCS})
target_link_libraries(concord-kvbc-proto protobuf::libprotobuf gRPC::grpc++)
target_include_directories(concord-kvbc-proto PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
