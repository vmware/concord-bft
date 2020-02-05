find_package(OpenSSL REQUIRED)

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
    RequestProcessingInfo.cpp
    messages/ClientPreProcessRequestMsg.cpp
    messages/PreProcessRequestMsg.cpp
    messages/PreProcessReplyMsg.cpp)

add_library(preprocessor STATIC ${preprocessor_source_files})

target_include_directories(preprocessor PUBLIC ${bftengine_SOURCE_DIR}/include)
target_include_directories(preprocessor PUBLIC ${bftengine_SOURCE_DIR}/include/bftengine)
target_include_directories(preprocessor PUBLIC ${bftengine_SOURCE_DIR}/src/bftengine)

target_link_libraries(preprocessor PUBLIC util)
target_link_libraries(preprocessor PUBLIC threshsign)
target_link_libraries(preprocessor PUBLIC ${OPENSSL_LIBRARIES})
