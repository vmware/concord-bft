add_subdirectory(config)
add_subdirectory(simpleKVBC)
add_subdirectory(simpleTest)
#[TODO-UTT] Make the utt demo compile after merge with master
# Note that libutt needs to compile for GCC to compile the demo in the CI.
#add_subdirectory(uttDemo)
if(RUN_APOLLO_TESTS)
    add_subdirectory(apollo)
endif()


