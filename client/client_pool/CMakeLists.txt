project("Concord Client Pool")

set(CMAKE_POSITION_INDEPENDENT_CODE ON)
add_library(concord_client_pool STATIC
        "src/concord_client_pool.cpp"
        "src/client_pool_config.cpp"
        "src/external_client.cpp"
        )
target_include_directories(concord_client_pool PUBLIC include)
target_link_libraries(concord_client_pool PUBLIC
        bftclient
        bftclient_new
        corebft
	concord_client_message
      	)

install (TARGETS concord_client_pool DESTINATION lib${LIB_SUFFIX})

if (BUILD_TESTING)
  add_subdirectory(test)
endif()
