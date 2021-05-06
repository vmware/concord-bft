find_package(Protobuf REQUIRED)
find_package(GRPC REQUIRED)

include_directories(${GRPC_INCLUDE_DIR})

# Avoid checking auto-generated code
# There is no generic way to avoid a file so we only enable one simple check
file(WRITE ${CMAKE_CURRENT_BINARY_DIR}/.clang-tidy "\
---
Checks:          '-*,misc-static-assert'
WarningsAsErrors: ''
AnalyzeTemporaryDtors: false
FormatStyle:    none
...
")

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
