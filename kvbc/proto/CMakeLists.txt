cmake_minimum_required(VERSION 3.5)
set(CMAKE_CXX_STANDARD 17)

find_package(absl CONFIG REQUIRED)
find_package(GRPC REQUIRED)
find_package(Protobuf CONFIG REQUIRED)

# Proto file
get_filename_component(concord_kvbc_proto "concord_kvbc.proto" ABSOLUTE)
get_filename_component(concord_kvbc_proto_path "${concord_kvbc_proto}" PATH)

set(ck_proto_srcs "${CMAKE_CURRENT_BINARY_DIR}/concord_kvbc.pb.cc")
set(ck_proto_hdrs "${CMAKE_CURRENT_BINARY_DIR}/concord_kvbc.pb.h")

add_custom_command(
      OUTPUT "${ck_proto_srcs}" "${ck_proto_hdrs}"
      COMMAND protoc
      ARGS --proto_path="${concord_kvbc_proto_path}"
        --cpp_out "${CMAKE_CURRENT_BINARY_DIR}"
        "${concord_kvbc_proto}"
      DEPENDS "${concord_kvbc_proto}")

message(STATUS "Concord KVBC protobuf generated - see " ${CMAKE_CURRENT_BINARY_DIR})

add_library(concord-kvbc-proto STATIC ${ck_proto_srcs})
target_link_libraries(concord-kvbc-proto protobuf::libprotobuf)
target_include_directories(concord-kvbc-proto PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
