add_subdirectory(config)
add_subdirectory(simpleKVBC)
add_subdirectory(simpleTest)
if ("${CMAKE_CXX_COMPILER_ID}" MATCHES "Clang")
    add_subdirectory(uttDemo)
endif()
if(RUN_APOLLO_TESTS)
    add_subdirectory(apollo)
endif()


