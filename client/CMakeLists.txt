project("Concord Client")

add_subdirectory("reconfiguration")
add_subdirectory("client_pool")

if(USE_GRPC)
	add_subdirectory("clientservice")
	add_subdirectory("concordclient")
	add_subdirectory("thin-replica-client")
	add_subdirectory("proto")
endif()