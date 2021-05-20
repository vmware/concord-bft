find_package(Protobuf REQUIRED)
find_package(GRPC REQUIRED)

include_directories(${GRPC_INCLUDE_DIR})

protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${CMAKE_CURRENT_BINARY_DIR}
  thin_replica.proto
)
grpc_generate_cpp(GRPC_SRCS GRPC_HDRS ${CMAKE_CURRENT_BINARY_DIR}
  thin_replica.proto
)
message(STATUS "Thin replica gRPC/protobuf generated - see " ${CMAKE_CURRENT_BINARY_DIR})

add_library(thin-replica-proto STATIC ${PROTO_SRCS} ${GRPC_SRCS})
target_link_libraries(thin-replica-proto protobuf::libprotobuf gRPC::grpc++)
target_include_directories(thin-replica-proto PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
