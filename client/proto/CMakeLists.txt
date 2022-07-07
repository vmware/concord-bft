cmake_minimum_required(VERSION 3.5)
set(CMAKE_CXX_STANDARD 17)

project(concord)

find_package(Protobuf REQUIRED)
find_package(absl CONFIG REQUIRED)
find_package(GRPC REQUIRED)

# Proto file
get_filename_component(rs_proto "request/v1/request.proto" ABSOLUTE)
get_filename_component(es_proto "event/v1/event.proto" ABSOLUTE)
get_filename_component(ss_proto "state_snapshot/v1/state_snapshot.proto" ABSOLUTE)
get_filename_component(ccr_proto "../concordclient/proto/concord_client_request/v1/concord_client_request.proto" ABSOLUTE)
get_filename_component(rs_proto_path "${rs_proto}" PATH)
get_filename_component(es_proto_path "${es_proto}" PATH)
get_filename_component(ss_proto_path "${ss_proto}" PATH)
get_filename_component(ccr_proto_path "${ccr_proto}" PATH)

get_target_property(grpc_cpp_plugin_location gRPC::grpc_cpp_plugin LOCATION)

# Generated sources
set(rs_proto_srcs "${CMAKE_CURRENT_BINARY_DIR}/request.pb.cc")
set(rs_proto_hdrs "${CMAKE_CURRENT_BINARY_DIR}/request.pb.h")
set(rs_grpc_srcs "${CMAKE_CURRENT_BINARY_DIR}/request.grpc.pb.cc")
set(rs_grpc_hdrs "${CMAKE_CURRENT_BINARY_DIR}/request.grpc.pb.h")
add_custom_command(
      OUTPUT "${rs_proto_srcs}" "${rs_proto_hdrs}" "${rs_grpc_srcs}" "${rs_grpc_hdrs}"
      COMMAND protoc
      ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}"
        --cpp_out "${CMAKE_CURRENT_BINARY_DIR}"
        -I "${rs_proto_path}"
        --plugin=protoc-gen-grpc="${grpc_cpp_plugin_location}"
        "${rs_proto}"
      DEPENDS "${rs_proto}")

set(es_proto_srcs "${CMAKE_CURRENT_BINARY_DIR}/event.pb.cc")
set(es_proto_hdrs "${CMAKE_CURRENT_BINARY_DIR}/event.pb.h")
set(es_grpc_srcs "${CMAKE_CURRENT_BINARY_DIR}/event.grpc.pb.cc")
set(es_grpc_hdrs "${CMAKE_CURRENT_BINARY_DIR}/event.grpc.pb.h")
add_custom_command(
      OUTPUT "${es_proto_srcs}" "${es_proto_hdrs}" "${es_grpc_srcs}" "${es_grpc_hdrs}"
      COMMAND protoc
      ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}"
        --cpp_out "${CMAKE_CURRENT_BINARY_DIR}"
        -I "${es_proto_path}"
        --plugin=protoc-gen-grpc="${grpc_cpp_plugin_location}"
        "${es_proto}"
      DEPENDS "${es_proto}")

set(ss_proto_srcs "${CMAKE_CURRENT_BINARY_DIR}/state_snapshot.pb.cc")
set(ss_proto_hdrs "${CMAKE_CURRENT_BINARY_DIR}/state_snapshot.pb.h")
set(ss_grpc_srcs "${CMAKE_CURRENT_BINARY_DIR}/state_snapshot.grpc.pb.cc")
set(ss_grpc_hdrs "${CMAKE_CURRENT_BINARY_DIR}/state_snapshot.grpc.pb.h")
add_custom_command(
      OUTPUT "${ss_proto_srcs}" "${ss_proto_hdrs}" "${ss_grpc_srcs}" "${ss_grpc_hdrs}"
      COMMAND protoc
      ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}"
        --cpp_out "${CMAKE_CURRENT_BINARY_DIR}"
        -I "${ss_proto_path}"
        --plugin=protoc-gen-grpc="${grpc_cpp_plugin_location}"
        "${ss_proto}"
      DEPENDS "${ss_proto}")

set(ccr_proto_srcs "${CMAKE_CURRENT_BINARY_DIR}/concord_client_request.pb.cc")
set(ccr_proto_hdrs "${CMAKE_CURRENT_BINARY_DIR}/concord_client_request.pb.h")
set(ccr_grpc_srcs "${CMAKE_CURRENT_BINARY_DIR}/concord_client_request.grpc.pb.cc")
set(ccr_grpc_hdrs "${CMAKE_CURRENT_BINARY_DIR}/concord_client_request.grpc.pb.h")
add_custom_command(
      OUTPUT "${ccr_proto_srcs}" "${ccr_proto_hdrs}" "${ccr_grpc_srcs}" "${ccr_grpc_hdrs}"
      COMMAND protoc
      ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}"
        --cpp_out "${CMAKE_CURRENT_BINARY_DIR}"
        -I "${ccr_proto_path}"
        --plugin=protoc-gen-grpc="${grpc_cpp_plugin_location}"
        "${ccr_proto}"
      DEPENDS "${ccr_proto}")


# Include generated *.pb.h files
include_directories("${CMAKE_CURRENT_BINARY_DIR}")

# clientservice-proto
add_library(clientservice-proto STATIC
  ${rs_grpc_srcs}
  ${rs_grpc_hdrs}
  ${rs_proto_srcs}
  ${rs_proto_hdrs}
  ${es_grpc_srcs}
  ${es_grpc_hdrs}
  ${es_proto_srcs}
  ${es_proto_hdrs}
  ${ss_grpc_srcs}
  ${ss_grpc_hdrs}
  ${ss_proto_srcs}
  ${ss_proto_hdrs}
  ${ccr_grpc_srcs}
  ${ccr_grpc_hdrs}
  ${ccr_proto_srcs}
  ${ccr_proto_hdrs})
target_link_libraries(clientservice-proto PRIVATE protobuf::libprotobuf gRPC::grpc++ absl::synchronization)
target_include_directories(clientservice-proto PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
