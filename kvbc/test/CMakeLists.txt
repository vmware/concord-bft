find_package(GTest REQUIRED)
find_package(rapidcheck)

if(Boost_FOUND)
    add_executable(blockchain_view_test blockchain_view_test.cpp $<TARGET_OBJECTS:logging_dev>)
    add_test(blockchain_view_test blockchain_view_test)
    target_include_directories(blockchain_view_test PRIVATE ${Boost_INCLUDE_DIRS})
    target_link_libraries(blockchain_view_test PUBLIC
        GTest::Main
        util
        kvbc
    )
endif()

add_executable(order_test order_test.cpp $<TARGET_OBJECTS:logging_dev>)
add_test(order_test order_test)
target_include_directories(order_test PUBLIC ${libkvbc_SOURCE_DIR}/include)
target_link_libraries(order_test GTest::Main util)
target_compile_options(order_test PUBLIC -Wno-sign-compare)

add_executable(sparse_merkle_storage_db_adapter_unit_test
    sparse_merkle_storage/db_adapter_unit_test.cpp $<TARGET_OBJECTS:logging_dev>)
add_test(sparse_merkle_storage_db_adapter_unit_test sparse_merkle_storage_db_adapter_unit_test)
target_link_libraries(sparse_merkle_storage_db_adapter_unit_test PUBLIC
    GTest::Main
    GTest::GTest
    util
    corebft
    kvbc
    stdc++fs
)

if(RAPIDCHECK_FOUND)
    add_executable(sparse_merkle_storage_db_adapter_property_test
        sparse_merkle_storage/db_adapter_property_test.cpp $<TARGET_OBJECTS:logging_dev>)
    add_test(sparse_merkle_storage_db_adapter_property_test sparse_merkle_storage_db_adapter_property_test)
    target_include_directories(sparse_merkle_storage_db_adapter_property_test PRIVATE ${RAPIDCHECK_INCLUDE_DIRS})
    target_link_libraries(sparse_merkle_storage_db_adapter_property_test PUBLIC
        GTest::Main
        GTest::GTest
        ${RAPIDCHECK_LIBRARIES}
        util
        corebft
        kvbc
        stdc++fs
    )
endif ()

add_executable(sparse_merkle_base_types_test sparse_merkle/base_types_test.cpp
    $<TARGET_OBJECTS:logging_dev>)
add_test(sparse_merkle_base_types_test sparse_merkle_base_types_test)
target_link_libraries(sparse_merkle_base_types_test PUBLIC
    GTest::Main
    GTest::GTest
    corebft
    kvbc
)

find_package(OpenSSL REQUIRED)
add_executable(sparse_merkle_internal_node_test sparse_merkle/internal_node_test.cpp
    $<TARGET_OBJECTS:logging_dev>)
add_test(sparse_merkle_internal_node_test sparse_merkle_internal_node_test)
target_link_libraries(sparse_merkle_internal_node_test PUBLIC
    GTest::Main
    GTest::GTest
    corebft
    kvbc
    ${OPENSSL_LIBRARIES}
)

find_package(OpenSSL REQUIRED)
add_executable(sparse_merkle_tree_test sparse_merkle/tree_test.cpp
    $<TARGET_OBJECTS:logging_dev>)
add_test(sparse_merkle_tree_test sparse_merkle_tree_test)
target_link_libraries(sparse_merkle_tree_test PUBLIC
    GTest::Main
    GTest::GTest
    kvbc
    corebft
    ${OPENSSL_LIBRARIES}
)

add_executable(multiIO_test multiIO_test.cpp $<TARGET_OBJECTS:logging_dev>)
add_test(multiIO_test multiIO_test)

target_link_libraries(multiIO_test PUBLIC
    GTest::Main
    GTest::GTest
    util
    kvbc
    corebft
)
