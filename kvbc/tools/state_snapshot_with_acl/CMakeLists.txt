find_package(Boost ${MIN_BOOST_VERSION} COMPONENTS program_options REQUIRED)

add_executable(state_snapshot_with_acl_tool state_snapshot_with_acl_tool.cpp)
target_link_libraries(state_snapshot_with_acl_tool PUBLIC
    kvbc
    util
    ${Boost_LIBRARIES}
)
