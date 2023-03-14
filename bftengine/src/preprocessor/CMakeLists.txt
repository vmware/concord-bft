find_package(yaml-cpp ${YAML_CPP_VERSION} REQUIRED)

set(preprocessor_source_files
    PreProcessor.cpp
    GlobalData.cpp
    RequestProcessingState.cpp
    messages/ClientPreProcessRequestMsg.cpp
    messages/ClientBatchRequestMsg.cpp
    messages/PreProcessRequestMsg.cpp
    messages/PreProcessBatchRequestMsg.cpp
    messages/PreProcessReplyMsg.cpp
    messages/PreProcessBatchReplyMsg.cpp
    messages/PreProcessResultMsg.cpp)

add_library(preprocessor STATIC ${preprocessor_source_files})

target_include_directories(preprocessor PUBLIC ${bftengine_SOURCE_DIR}/include)
target_include_directories(preprocessor PUBLIC ${bftengine_SOURCE_DIR}/include/bftengine)
target_include_directories(preprocessor PUBLIC ${bftengine_SOURCE_DIR}/src/bftengine)
get_property(perf_include GLOBAL PROPERTY PERF_MANAGER_INCLUDE_DIR)
get_property(util_include GLOBAL PROPERTY UTIL_INCLUDE_DIR)
get_property(kvbc_include GLOBAL PROPERTY KVBC_INCLUDE_DIR)
target_include_directories(preprocessor PUBLIC ${perf_include} ${util_include} ${kvbc_include})

if(BUILD_SLOWDOWN)
    target_compile_definitions(preprocessor PUBLIC USE_SLOWDOWN)
endif()

target_link_libraries(preprocessor PUBLIC diagnostics util bftcommunication yaml-cpp)

if (BUILD_TESTING)
    add_subdirectory(tests)
endif()
