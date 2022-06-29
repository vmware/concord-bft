find_package(Boost ${MIN_BOOST_VERSION} COMPONENTS program_options REQUIRED)

add_executable(block_merkle_latest_ver_cf_migration_tool block_merkle_latest_ver_cf_migration_tool.cpp)
target_link_libraries(block_merkle_latest_ver_cf_migration_tool PUBLIC
    kvbc
    util
    ${Boost_LIBRARIES}
)

add_executable(v4migration_tool
        v4migration_tool/src/v4migration_tool.cpp
        v4migration_tool/src/migration_bookeeper.cpp)

target_include_directories(v4migration_tool PUBLIC "v4migration_tool/include" )

target_link_libraries(v4migration_tool PUBLIC
        kvbc
        util
        ${Boost_LIBRARIES}
)

