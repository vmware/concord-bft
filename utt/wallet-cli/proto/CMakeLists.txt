cmake_minimum_required(VERSION 3.5)
set(CMAKE_CXX_STANDARD 17)

project(concord)

find_package(Protobuf REQUIRED)
find_package(GRPC REQUIRED)

include_directories(${GRPC_INCLUDE_DIR})

# Proto file
get_filename_component(utt_wallet_api_proto "api/v1/api.proto" ABSOLUTE)
get_filename_component(utt_wallet_api_proto_path "${utt_wallet_api_proto}" PATH)

get_target_property(grpc_cpp_plugin_location gRPC::grpc_cpp_plugin LOCATION)

set(utt_wallet_api_proto_srcs "${CMAKE_CURRENT_BINARY_DIR}/api.pb.cc")
set(utt_wallet_api_proto_hdrs "${CMAKE_CURRENT_BINARY_DIR}/api.pb.h")
set(utt_wallet_api_grpc_srcs "${CMAKE_CURRENT_BINARY_DIR}/api.grpc.pb.cc")
set(utt_wallet_api_grpc_hdrs "${CMAKE_CURRENT_BINARY_DIR}/api.grpc.pb.h")

add_custom_command(
      OUTPUT "${utt_wallet_api_proto_srcs}" "${utt_wallet_api_proto_hdrs}" "${utt_wallet_api_grpc_srcs}" "${utt_wallet_api_grpc_hdrs}"
      COMMAND protoc
      ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}"
        --cpp_out "${CMAKE_CURRENT_BINARY_DIR}"
        -I "${utt_wallet_api_proto_path}"
        --plugin=protoc-gen-grpc="${grpc_cpp_plugin_location}"
        "${utt_wallet_api_proto}"
      DEPENDS "${utt_wallet_api_proto}")

add_library(utt-wallet-api-proto STATIC ${utt_wallet_api_proto_srcs} ${utt_wallet_api_grpc_srcs})
target_link_libraries(utt-wallet-api-proto PRIVATE protobuf::libprotobuf gRPC::grpc++)
target_include_directories(utt-wallet-api-proto PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
