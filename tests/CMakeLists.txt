add_subdirectory(config)
add_subdirectory(simpleKVBC)
add_subdirectory(simpleTest)
add_subdirectory(uttDemo)
if(RUN_APOLLO_TESTS)
    add_subdirectory(apollo)
endif()


