add_subdirectory(config)
add_subdirectory(simpleKVBC)
add_subdirectory(simpleTest)
if(RUN_APOLLO_TESTS)
    add_subdirectory(apollo)
endif()


