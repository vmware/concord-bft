cmake_minimum_required(VERSION 3.5)
set(CMAKE_CXX_STANDARD 17)

find_package(GRPC REQUIRED)
find_package(absl CONFIG REQUIRED)
find_package(Protobuf CONFIG REQUIRED)

include_directories(${GRPC_INCLUDE_DIR})

get_filename_component(tr_proto "thin_replica.proto" ABSOLUTE)
get_filename_component(tr_proto_path "${tr_proto}" PATH)

get_target_property(grpc_cpp_plugin_location gRPC::grpc_cpp_plugin LOCATION)

set(tr_proto_srcs "${CMAKE_CURRENT_BINARY_DIR}/thin_replica.pb.cc")
set(tr_proto_hdrs "${CMAKE_CURRENT_BINARY_DIR}/thin_replica.pb.h")
set(tr_grpc_srcs "${CMAKE_CURRENT_BINARY_DIR}/thin_replica.grpc.pb.cc")
set(tr_grpc_hdrs "${CMAKE_CURRENT_BINARY_DIR}/thin_replica.grpc.pb.h")
add_custom_command(
      OUTPUT "${tr_proto_srcs}" "${tr_proto_hdrs}" "${tr_grpc_srcs}" "${tr_grpc_hdrs}"
      COMMAND protoc
      ARGS --grpc_out=generate_mock_code=true:"${CMAKE_CURRENT_BINARY_DIR}"
        --cpp_out "${CMAKE_CURRENT_BINARY_DIR}"
        -I "${tr_proto_path}"
        --plugin=protoc-gen-grpc="${grpc_cpp_plugin_location}"
        "${tr_proto}"
      DEPENDS "${tr_proto}")

message(STATUS "Thin replica gRPC/protobuf generated - see " ${CMAKE_CURRENT_BINARY_DIR})

get_filename_component(ss_proto "replica_state_snapshot.proto" ABSOLUTE)
get_filename_component(ss_proto_path "${ss_proto}" PATH)

set(ss_proto_srcs "${CMAKE_CURRENT_BINARY_DIR}/replica_state_snapshot.pb.cc")
set(ss_proto_hdrs "${CMAKE_CURRENT_BINARY_DIR}/replica_state_snapshot.pb.h")
set(ss_grpc_srcs "${CMAKE_CURRENT_BINARY_DIR}/replica_state_snapshot.grpc.pb.cc")
set(ss_grpc_hdrs "${CMAKE_CURRENT_BINARY_DIR}/replica_state_snapshot.grpc.pb.h")
add_custom_command(
      OUTPUT "${ss_proto_srcs}" "${ss_proto_hdrs}" "${ss_grpc_srcs}" "${ss_grpc_hdrs}"
      COMMAND protoc
      ARGS --grpc_out=generate_mock_code=true:"${CMAKE_CURRENT_BINARY_DIR}"
        --cpp_out "${CMAKE_CURRENT_BINARY_DIR}"
        -I "${ss_proto_path}"
        --plugin=protoc-gen-grpc="${grpc_cpp_plugin_location}"
        "${ss_proto}"
      DEPENDS "${ss_proto}")

message(STATUS "State snapshot gRPC/protobuf generated - see " ${CMAKE_CURRENT_BINARY_DIR})

add_library(thin-replica-proto STATIC ${tr_proto_srcs} ${tr_proto_hdrs} 
            ${tr_grpc_srcs} ${tr_grpc_hdrs})
target_link_libraries(thin-replica-proto protobuf::libprotobuf gRPC::grpc++ gRPC::grpc++_reflection)
target_include_directories(thin-replica-proto PUBLIC ${CMAKE_CURRENT_BINARY_DIR})

add_library(replica-state-snapshot-proto STATIC ${ss_proto_srcs} ${ss_proto_hdrs} 
            ${ss_grpc_srcs} ${ss_grpc_hdrs})
target_link_libraries(replica-state-snapshot-proto protobuf::libprotobuf gRPC::grpc++ gRPC::grpc++_reflection)
target_include_directories(replica-state-snapshot-proto PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
