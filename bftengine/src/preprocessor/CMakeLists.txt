find_package(OpenSSL REQUIRED)
find_package(cryptopp REQUIRED)

if (BUILD_TESTING)
    add_subdirectory(tests)
endif()

set(preprocessor_source_files
    ${bftengine_SOURCE_DIR}/src/bftengine/MsgsCommunicator.cpp
    ${bftengine_SOURCE_DIR}/src/bftengine/SigManager.cpp
    ${bftengine_SOURCE_DIR}/src/bftengine/Crypto.cpp
    ${bftengine_SOURCE_DIR}/src/bftengine/messages/ClientRequestMsg.cpp
    ${bftengine_SOURCE_DIR}/src/bftengine/messages/MessageBase.cpp
    PreProcessor.cpp
    RequestProcessingState.cpp
    messages/ClientPreProcessRequestMsg.cpp
    messages/ClientBatchRequestMsg.cpp
    messages/PreProcessRequestMsg.cpp
    messages/PreProcessBatchRequestMsg.cpp
    messages/PreProcessReplyMsg.cpp)

add_library(preprocessor STATIC ${preprocessor_source_files})

target_include_directories(preprocessor PUBLIC ${CRYPTOPP_INCLUDE_DIRS})
target_include_directories(preprocessor PUBLIC ${OPENSSL_INCLUDE_DIR})
target_include_directories(preprocessor PUBLIC ${bftengine_SOURCE_DIR}/include)
target_include_directories(preprocessor PUBLIC ${CMAKE_CURRENT_BINARY_DIR}/../../cmf)
target_include_directories(preprocessor PUBLIC ${bftengine_SOURCE_DIR}/include/bftengine)
target_include_directories(preprocessor PUBLIC ${bftengine_SOURCE_DIR}/src/bftengine)
get_property(perf_include GLOBAL PROPERTY PERF_MANAGER_INCLUDE_DIR)
get_property(util_include GLOBAL PROPERTY UTIL_INCLUDE_DIR)
get_property(kvbc_include GLOBAL PROPERTY KVBC_INCLUDE_DIR)
target_include_directories(preprocessor PUBLIC ${perf_include} ${util_include} ${kvbc_include})

target_link_libraries(preprocessor PUBLIC diagnostics)

if(BUILD_SLOWDOWN)
    target_compile_definitions(preprocessor PUBLIC USE_SLOWDOWN)
endif()

target_link_libraries(preprocessor PUBLIC util)
target_link_libraries(preprocessor PUBLIC threshsign)
target_link_libraries(preprocessor PUBLIC ${OPENSSL_LIBRARIES})
target_link_libraries(preprocessor PUBLIC bftcommunication)