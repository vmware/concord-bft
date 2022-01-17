find_package(Protobuf REQUIRED)
find_package(GRPC REQUIRED)

include_directories(${GRPC_INCLUDE_DIR})

protobuf_generate_cpp(THIN_REPLICA_PROTO_SRCS THIN_REPLICA_PROTO_HDRS ${CMAKE_CURRENT_BINARY_DIR}
  thin_replica.proto
)
grpc_generate_cpp(THIN_REPLICA_GRPC_SRCS THIN_REPLICA_GRPC_HDRS ${CMAKE_CURRENT_BINARY_DIR}
  thin_replica.proto
)
message(STATUS "Thin replica gRPC/protobuf generated - see " ${CMAKE_CURRENT_BINARY_DIR})

protobuf_generate_cpp(STATE_SNAPSHOT_PROTO_SRCS STATE_SNAPSHOT_PROTO_HDRS ${CMAKE_CURRENT_BINARY_DIR}
  replica_state_snapshot.proto
)
grpc_generate_cpp(STATE_SNAPSHOT_GRPC_SRCS STATE_SNAPSHOT_GRPC_HDRS ${CMAKE_CURRENT_BINARY_DIR}
  replica_state_snapshot.proto
)
message(STATUS "State snapshot gRPC/protobuf generated - see " ${CMAKE_CURRENT_BINARY_DIR})

add_library(thin-replica-proto STATIC ${THIN_REPLICA_PROTO_SRCS} ${THIN_REPLICA_PROTO_HDRS} 
            ${THIN_REPLICA_GRPC_SRCS} ${THIN_REPLICA_GRPC_HDRS})
target_link_libraries(thin-replica-proto protobuf::libprotobuf gRPC::grpc++)
target_include_directories(thin-replica-proto PUBLIC ${CMAKE_CURRENT_BINARY_DIR})

add_library(state-snapshot-proto STATIC ${STATE_SNAPSHOT_PROTO_SRCS} ${STATE_SNAPSHOT_PROTO_HDRS} 
            ${STATE_SNAPSHOT_GRPC_SRCS} ${STATE_SNAPSHOT_GRPC_HDRS})
target_link_libraries(state-snapshot-proto protobuf::libprotobuf gRPC::grpc++)
target_include_directories(state-snapshot-proto PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
