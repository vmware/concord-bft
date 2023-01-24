find_package(Protobuf REQUIRED)
find_package(GRPC REQUIRED)

include_directories(${GRPC_INCLUDE_DIR})

protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${CMAKE_CURRENT_BINARY_DIR}
  api/v1/wallet-api.proto
)
grpc_generate_cpp(GRPC_SRCS GRPC_HDRS ${CMAKE_CURRENT_BINARY_DIR}
  api/v1/wallet-api.proto
)

add_library(privacy-wallet-service-proto STATIC ${PROTO_SRCS} ${GRPC_SRCS})
target_link_libraries(privacy-wallet-service-proto PRIVATE protobuf::libprotobuf gRPC::grpc++)
target_include_directories(privacy-wallet-service-proto PUBLIC ${CMAKE_CURRENT_BINARY_DIR})