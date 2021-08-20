add_executable(ClientRequestMsg_test ClientRequestMsg_test.cpp helper.cpp)
add_test(ClientRequestMsg_test ClientRequestMsg_test)
find_package(GTest REQUIRED)
target_include_directories(ClientRequestMsg_test
      PRIVATE
      ${bftengine_SOURCE_DIR}/src/bftengine)
target_link_libraries(ClientRequestMsg_test GTest::Main)
target_link_libraries(ClientRequestMsg_test corebft )
target_compile_options(ClientRequestMsg_test PUBLIC "-Wno-sign-compare")


add_executable(PrePrepareMsg_test PrePrepareMsg_test.cpp helper.cpp)
add_test(PrePrepareMsg_test PrePrepareMsg_test)
find_package(GTest REQUIRED)
target_include_directories(PrePrepareMsg_test
      PRIVATE
      ${bftengine_SOURCE_DIR}/src/bftengine)
target_link_libraries(PrePrepareMsg_test GTest::Main)
target_link_libraries(PrePrepareMsg_test corebft )
target_compile_options(PrePrepareMsg_test PUBLIC "-Wno-sign-compare")

add_executable(PartialCommitProofMsg_test PartialCommitProofMsg_test.cpp helper.cpp)
add_test(PartialCommitProofMsg_test PartialCommitProofMsg_test )
find_package(GTest REQUIRED)
target_include_directories(PartialCommitProofMsg_test
      PRIVATE
      ${bftengine_SOURCE_DIR}/src/bftengine)
target_link_libraries(PartialCommitProofMsg_test GTest::Main)
target_link_libraries(PartialCommitProofMsg_test corebft )
target_compile_options(PartialCommitProofMsg_test PUBLIC "-Wno-sign-compare")

add_executable(SignedShareBase_test SignedShareBase_test.cpp helper.cpp)
add_test(SignedShareBase_test SignedShareBase_test)
find_package(GTest REQUIRED)
target_include_directories(SignedShareBase_test
      PRIVATE
      ${bftengine_SOURCE_DIR}/src/bftengine)
target_link_libraries(SignedShareBase_test GTest::Main)
target_link_libraries(SignedShareBase_test corebft )
target_compile_options(SignedShareBase_test PUBLIC "-Wno-sign-compare")

add_executable(CheckpointMsg_test CheckpointMsg_test.cpp helper.cpp)
add_test(CheckpointMsg_test CheckpointMsg_test)
find_package(GTest REQUIRED)
target_include_directories(CheckpointMsg_test
      PRIVATE
      ${bftengine_SOURCE_DIR}/src/bftengine)
target_link_libraries(CheckpointMsg_test GTest::Main)
target_link_libraries(CheckpointMsg_test corebft )
target_compile_options(CheckpointMsg_test PUBLIC "-Wno-sign-compare")

add_executable(ReqMissingDataMsg_test ReqMissingDataMsg_test.cpp helper.cpp)
add_test(ReqMissingDataMsg_test ReqMissingDataMsg_test)
find_package(GTest REQUIRED)
target_include_directories(ReqMissingDataMsg_test
      PRIVATE
      ${bftengine_SOURCE_DIR}/src/bftengine)
target_link_libraries(ReqMissingDataMsg_test GTest::Main)
target_link_libraries(ReqMissingDataMsg_test corebft )
target_compile_options(ReqMissingDataMsg_test PUBLIC "-Wno-sign-compare")

add_executable(ViewChangeMsg_test ViewChangeMsg_test.cpp helper.cpp)
add_test(ViewChangeMsg_test ViewChangeMsg_test)
find_package(GTest REQUIRED)
target_include_directories(ViewChangeMsg_test
      PRIVATE
      ${bftengine_SOURCE_DIR}/src/bftengine)
target_link_libraries(ViewChangeMsg_test GTest::Main)
target_link_libraries(ViewChangeMsg_test corebft )
target_compile_options(ViewChangeMsg_test PUBLIC "-Wno-sign-compare")

add_executable(StartSlowCommitMsg_test StartSlowCommitMsg_test.cpp helper.cpp)
add_test(StartSlowCommitMsg_test StartSlowCommitMsg_test)
find_package(GTest REQUIRED)
target_include_directories(StartSlowCommitMsg_test
      PRIVATE
      ${bftengine_SOURCE_DIR}/src/bftengine)
target_link_libraries(StartSlowCommitMsg_test GTest::Main)
target_link_libraries(StartSlowCommitMsg_test corebft )
target_compile_options(StartSlowCommitMsg_test PUBLIC "-Wno-sign-compare")

add_executable(ReplicaStatusMsg_test ReplicaStatusMsg_test.cpp helper.cpp)
add_test(ReplicaStatusMsg_test ReplicaStatusMsg_test)
find_package(GTest REQUIRED)
target_include_directories(ReplicaStatusMsg_test
      PRIVATE
      ${bftengine_SOURCE_DIR}/src/bftengine)
target_link_libraries(ReplicaStatusMsg_test GTest::Main)
target_link_libraries(ReplicaStatusMsg_test corebft )
target_compile_options(ReplicaStatusMsg_test PUBLIC "-Wno-sign-compare")

add_executable(AskForCheckpointMsg_test AskForCheckpointMsg_test.cpp helper.cpp)
add_test(AskForCheckpointMsg_test AskForCheckpointMsg_test)
find_package(GTest REQUIRED)
target_include_directories(AskForCheckpointMsg_test
      PRIVATE
      ${bftengine_SOURCE_DIR}/src/bftengine)
target_link_libraries(AskForCheckpointMsg_test GTest::Main)
target_link_libraries(AskForCheckpointMsg_test corebft )
target_compile_options(AskForCheckpointMsg_test PUBLIC "-Wno-sign-compare")

add_executable(FullCommitProofMsg_test FullCommitProofMsg_test.cpp helper.cpp)
add_test(FullCommitProofMsg_test FullCommitProofMsg_test)
find_package(GTest REQUIRED)
target_include_directories(FullCommitProofMsg_test
      PRIVATE
      ${bftengine_SOURCE_DIR}/src/bftengine)
target_link_libraries(FullCommitProofMsg_test GTest::Main)
target_link_libraries(FullCommitProofMsg_test corebft )
target_compile_options(FullCommitProofMsg_test PUBLIC "-Wno-sign-compare")

add_executable(NewViewMsg_test NewViewMsg_test.cpp helper.cpp)
add_test(NewViewMsg_test NewViewMsg_test)
find_package(GTest REQUIRED)
target_include_directories(NewViewMsg_test
      PRIVATE
      ${bftengine_SOURCE_DIR}/src/bftengine)
target_link_libraries(NewViewMsg_test GTest::Main)
target_link_libraries(NewViewMsg_test corebft )
target_compile_options(NewViewMsg_test PUBLIC "-Wno-sign-compare")

add_executable(ReplicaAsksToLeaveViewMsg_test ReplicaAsksToLeaveViewMsg_test.cpp helper.cpp)
add_test(ReplicaAsksToLeaveViewMsg_test ReplicaAsksToLeaveViewMsg_test)
find_package(GTest REQUIRED)
target_include_directories(ReplicaAsksToLeaveViewMsg_test
      PRIVATE
      ${bftengine_SOURCE_DIR}/src/bftengine)
target_link_libraries(ReplicaAsksToLeaveViewMsg_test GTest::Main)
target_link_libraries(ReplicaAsksToLeaveViewMsg_test corebft )
target_compile_options(ReplicaAsksToLeaveViewMsg_test PUBLIC "-Wno-sign-compare")

add_executable(ReplicaRestartReadyMsg_test ReplicaRestartReadyMsg_test.cpp helper.cpp)
add_test(ReplicaRestartReadyMsg_test ReplicaRestartReadyMsg_test)
find_package(GTest REQUIRED)
target_include_directories(ReplicaRestartReadyMsg_test
      PRIVATE
      ${bftengine_SOURCE_DIR}/src/bftengine)
target_link_libraries(ReplicaRestartReadyMsg_test GTest::Main)
target_link_libraries(ReplicaRestartReadyMsg_test corebft )
target_compile_options(ReplicaRestartReadyMsg_test PUBLIC "-Wno-sign-compare")

add_executable(InstallReadyMsg_test InstallReadyMsg_test.cpp helper.cpp)
add_test(InstallReadyMsg_test InstallReadyMsg_test)
find_package(GTest REQUIRED)
target_include_directories(InstallReadyMsg_test
      PRIVATE
      ${bftengine_SOURCE_DIR}/src/bftengine)
target_link_libraries(InstallReadyMsg_test GTest::Main)
target_link_libraries(InstallReadyMsg_test corebft )
target_compile_options(InstallReadyMsg_test PUBLIC "-Wno-sign-compare")