find_package(Protobuf REQUIRED)
find_package(gRPC REQUIRED)

include_directories(${GRPC_INCLUDE_DIR})

protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS api/v1/admin-api.proto)
grpc_generate_cpp(GRPC_SRCS GRPC_HDRS ${CMAKE_CURRENT_BINARY_DIR} api/v1/admin-api.proto)

add_library(privacy-admin-api-proto STATIC ${PROTO_SRCS} ${GRPC_SRCS})
target_link_libraries(privacy-admin-api-proto PRIVATE protobuf::libprotobuf gRPC::grpc++)
target_include_directories(privacy-admin-api-proto PUBLIC ${CMAKE_CURRENT_BINARY_DIR})