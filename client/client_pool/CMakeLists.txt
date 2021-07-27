project("Concord Client Pool")
# Require C++17.
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
include_directories(${CMAKE_CURRENT_BINARY_DIR}/submodules)
include_directories(${CMAKE_CURRENT_BINARY_DIR})
include_directories(.)
include_directories(../../threshsign/include/)
include_directories(../../bftengine/include/bcstatetransfer/)
add_library(concord_client_pool STATIC
        "src/concord_client_pool.cpp"
        "src/client_pool_config.cpp"
        "src/external_client.cpp"
        )
get_property(thresh_include GLOBAL PROPERTY thresh_include_folder)
target_include_directories(concord_client_pool PRIVATE ${thresh_include})
target_include_directories(concord_client_pool PUBLIC ${perf_include})
target_include_directories(concord_client_pool PUBLIC ${CMAKE_CURRENT_BINARY_DIR}/../../bftengine/cmf)
target_include_directories(concord_client_pool PUBLIC include)
target_link_libraries(concord_client_pool PUBLIC
        ${Boost_LIBRARIES}
        ${yaml-cpp_LIBRARIES}
        logging
        bftclient
        bftclient_new
        kvbc
        util
        threshsign
        secretsmanager)

install (TARGETS concord_client_pool DESTINATION lib${LIB_SUFFIX})
