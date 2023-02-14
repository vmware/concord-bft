add_subdirectory(config)
add_subdirectory(simpleKVBC)
add_subdirectory(simpleTest)
add_subdirectory(metric_server)
if(RUN_APOLLO_TESTS)
    add_subdirectory(apollo)
endif()


