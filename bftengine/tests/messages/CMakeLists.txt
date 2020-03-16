add_executable(ClientRequestMsg_test ClientRequestMsg_test.cpp)
add_test(ClientRequestMsg_test ClientRequestMsg_test)
find_package(GTest REQUIRED)
target_include_directories(ClientRequestMsg_test
      PRIVATE
      ${bftengine_SOURCE_DIR}/src/bftengine)
target_link_libraries(ClientRequestMsg_test GTest::Main)
target_link_libraries(ClientRequestMsg_test corebft $<TARGET_OBJECTS:logging_dev>)
target_compile_options(ClientRequestMsg_test PUBLIC "-Wno-sign-compare")


add_executable(PrePrepareMsg_test PrePrepareMsg_test.cpp)
add_test(PrePrepareMsg_test PrePrepareMsg_test)
find_package(GTest REQUIRED)
target_include_directories(PrePrepareMsg_test
      PRIVATE
      ${bftengine_SOURCE_DIR}/src/bftengine)
target_link_libraries(PrePrepareMsg_test GTest::Main)
target_link_libraries(PrePrepareMsg_test corebft $<TARGET_OBJECTS:logging_dev>)
target_compile_options(PrePrepareMsg_test PUBLIC "-Wno-sign-compare")

add_executable(PartialCommitProofMsg_test PartialCommitProofMsg_test.cpp)
add_test(PartialCommitProofMsg_test PartialCommitProofMsg_test )
find_package(GTest REQUIRED)
target_include_directories(PartialCommitProofMsg_test
      PRIVATE
      ${bftengine_SOURCE_DIR}/src/bftengine)
target_link_libraries(PartialCommitProofMsg_test GTest::Main)
target_link_libraries(PartialCommitProofMsg_test corebft $<TARGET_OBJECTS:logging_dev>)
target_compile_options(PartialCommitProofMsg_test PUBLIC "-Wno-sign-compare")

add_executable(SignedShareBase_test SignedShareBase_test.cpp)
add_test(SignedShareBase_test SignedShareBase_test)
find_package(GTest REQUIRED)
target_include_directories(SignedShareBase_test
      PRIVATE
      ${bftengine_SOURCE_DIR}/src/bftengine)
target_link_libraries(SignedShareBase_test GTest::Main)
target_link_libraries(SignedShareBase_test corebft $<TARGET_OBJECTS:logging_dev>)
target_compile_options(SignedShareBase_test PUBLIC "-Wno-sign-compare")

add_executable(CheckpointMsg_test CheckpointMsg_test.cpp)
add_test(CheckpointMsg_test CheckpointMsg_test)
find_package(GTest REQUIRED)
target_include_directories(CheckpointMsg_test
      PRIVATE
      ${bftengine_SOURCE_DIR}/src/bftengine)
target_link_libraries(CheckpointMsg_test GTest::Main)
target_link_libraries(CheckpointMsg_test corebft $<TARGET_OBJECTS:logging_dev>)
target_compile_options(CheckpointMsg_test PUBLIC "-Wno-sign-compare")

add_executable(PartialExecProofMsg_test PartialExecProofMsg_test.cpp)
add_test(PartialExecProofMsg_test PartialExecProofMsg_test)
find_package(GTest REQUIRED)
target_include_directories(PartialExecProofMsg_test
      PRIVATE
      ${bftengine_SOURCE_DIR}/src/bftengine)
target_link_libraries(PartialExecProofMsg_test GTest::Main)
target_link_libraries(PartialExecProofMsg_test corebft $<TARGET_OBJECTS:logging_dev>)
target_compile_options(PartialExecProofMsg_test PUBLIC "-Wno-sign-compare")

add_executable(ReqMissingDataMsg_test ReqMissingDataMsg_test.cpp)
add_test(ReqMissingDataMsg_test ReqMissingDataMsg_test)
find_package(GTest REQUIRED)
target_include_directories(ReqMissingDataMsg_test
      PRIVATE
      ${bftengine_SOURCE_DIR}/src/bftengine)
target_link_libraries(ReqMissingDataMsg_test GTest::Main)
target_link_libraries(ReqMissingDataMsg_test corebft $<TARGET_OBJECTS:logging_dev>)
target_compile_options(ReqMissingDataMsg_test PUBLIC "-Wno-sign-compare")

add_executable(StartSlowCommitMsg_test StartSlowCommitMsg_test.cpp)
add_test(StartSlowCommitMsg_test StartSlowCommitMsg_test)
find_package(GTest REQUIRED)
target_include_directories(StartSlowCommitMsg_test
      PRIVATE
      ${bftengine_SOURCE_DIR}/src/bftengine)
target_link_libraries(StartSlowCommitMsg_test GTest::Main)
target_link_libraries(StartSlowCommitMsg_test corebft $<TARGET_OBJECTS:logging_dev>)
target_compile_options(StartSlowCommitMsg_test PUBLIC "-Wno-sign-compare")
