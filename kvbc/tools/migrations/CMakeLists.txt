find_package(Boost ${MIN_BOOST_VERSION} COMPONENTS program_options REQUIRED)

add_executable(block_merkle_latest_ver_cf_migration_tool block_merkle_latest_ver_cf_migration_tool.cpp)
target_link_libraries(block_merkle_latest_ver_cf_migration_tool PUBLIC
    kvbc
    util
    ${Boost_LIBRARIES}
)
