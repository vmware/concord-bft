cmake_minimum_required (VERSION 3.2)
project(libkvbc VERSION 0.1.0.0 LANGUAGES CXX)

find_package(Boost ${MIN_BOOST_VERSION})

add_library(
    kvbc
    src/ClientImp.cpp
    src/ReplicaImp.cpp
    src/replica_state_sync_imp.cpp
    src/block_metadata.cpp
    src/ror_app_state.cpp)

target_link_libraries(kvbc PUBLIC corebft threshsign util concordbft_storage object_store_lib)

target_include_directories(kvbc PUBLIC include)

if (BUILD_TESTING)
    add_subdirectory(test)
endif()
