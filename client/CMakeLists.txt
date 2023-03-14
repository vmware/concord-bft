project("Concord Client")

add_subdirectory("bftclient")
add_subdirectory("reconfiguration")
add_subdirectory("client_pool")

if(USE_GRPC)
  if (USE_PROMETHEUS AND USE_JAEGER)
    add_subdirectory("clientservice")
  endif()
  add_subdirectory("concordclient")
  add_subdirectory("thin-replica-client")
  add_subdirectory("proto")
endif()
