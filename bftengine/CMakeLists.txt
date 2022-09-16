project (bftengine LANGUAGES CXX)

add_subdirectory(src/preprocessor)
find_package(CMFC REQUIRED)
add_subdirectory("cmf")

set(corebft_source_files
    src/bftengine/PrimitiveTypes.cpp
    src/bftengine/DebugStatistics.cpp
    src/bftengine/SeqNumInfo.cpp
    src/bftengine/ReadOnlyReplica.cpp
    src/bftengine/ReplicaBase.cpp
    src/bftengine/ReplicaForStateTransfer.cpp
    src/bftengine/ReplicaImp.cpp
    src/bftengine/ControllerBase.cpp
    src/bftengine/ControllerWithSimpleHistory.cpp
    src/bftengine/IncomingMsgsStorageImp.cpp
    src/bftengine/RetransmissionsManager.cpp
    src/bftengine/SigManager.cpp
    src/bftengine/ReplicasInfo.cpp
    src/bftengine/ViewChangeSafetyLogic.cpp
    src/bftengine/ViewsManager.cpp
    src/bftengine/ClientsManager.cpp
    src/bftengine/NullStateTransfer.cpp
    src/bftengine/BFTEngine.cpp
    src/bftengine/SimpleClientImp.cpp
    src/bftengine/PersistentStorageImp.cpp
    src/bftengine/PersistentStorageDescriptors.cpp
    src/bftengine/PersistentStorageWindows.cpp
    src/bftengine/DebugPersistentStorage.cpp
    src/bftengine/ReplicaLoader.cpp
    src/bftengine/SerializableActiveWindow.cpp
    src/bftengine/MsgsCommunicator.cpp
    src/bftengine/MsgReceiver.cpp
    src/bftengine/DbMetadataStorage.cpp
    src/bftengine/RequestsBatchingLogic.cpp
    src/bftengine/ReplicaStatusHandlers.cpp
    src/bcstatetransfer/BCStateTran.cpp
    src/bcstatetransfer/BCStateTranInterface.cpp
    src/bcstatetransfer/RVBManager.cpp
    src/bcstatetransfer/InMemoryDataStore.cpp
    src/bcstatetransfer/DBDataStore.cpp
    src/bcstatetransfer/SourceSelector.cpp
    src/bcstatetransfer/AsyncStateTransferCRE.cpp
    src/bcstatetransfer/RangeValidationTree.cpp
    src/simplestatetransfer/SimpleStateTran.cpp
    src/bftengine/messages/PrePrepareMsg.cpp
    src/bftengine/messages/CheckpointMsg.cpp
    src/bftengine/messages/FullCommitProofMsg.cpp
    src/bftengine/messages/MessageBase.cpp
    src/bftengine/messages/PartialCommitProofMsg.cpp
    src/bftengine/messages/ClientReplyMsg.cpp
    src/bftengine/messages/ReqMissingDataMsg.cpp
    src/bftengine/messages/ClientRequestMsg.cpp
    src/bftengine/messages/StartSlowCommitMsg.cpp
    src/bftengine/messages/SignedShareMsgs.cpp
    src/bftengine/messages/SimpleAckMsg.cpp
    src/bftengine/messages/NewViewMsg.cpp
    src/bftengine/messages/ViewChangeMsg.cpp
    src/bftengine/messages/ReplicaStatusMsg.cpp
    src/bftengine/messages/StateTransferMsg.cpp
    src/bftengine/messages/ReplicaRestartReadyMsg.cpp
    src/bftengine/messages/ReplicasRestartReadyProofMsg.cpp
    src/bftengine/messages/ReplicaAsksToLeaveViewMsg.cpp
    src/bftengine/KeyExchangeManager.cpp
    src/bftengine/RequestHandler.cpp
    src/bftengine/ControlStateManager.cpp
    src/bftengine/InternalBFTClient.cpp
    src/bftengine/KeyStore.cpp
    src/bftengine/TimeServiceResPageClient.cpp
    src/bftengine/DbCheckpointManager.cpp
    src/bftengine/ReplicaSpecificInfoManager.cpp
    ../ccron/src/ticks_generator.cpp
    ../ccron/src/periodic_action.cpp
    ../ccron/src/cron_table.cpp
)

#
# pthread dependency
find_package(Threads REQUIRED)
#message("Threads library: ${CMAKE_THREAD_LIBS_INIT}")


#
# Targets
#
add_library(corebft STATIC ${corebft_source_files})
add_library(bftclient STATIC src/bftengine/SimpleClientImp)
add_library(bftheaders INTERFACE)
target_include_directories(bftheaders INTERFACE include)

get_property(perf_include GLOBAL PROPERTY PERF_MANAGER_INCLUDE_DIR)
get_property(kvbc_include GLOBAL PROPERTY KVBC_INCLUDE_DIR)

target_include_directories(corebft PUBLIC include/)
target_include_directories(corebft PUBLIC include/bftengine)
target_include_directories(corebft PUBLIC include/bcstatetransfer)
target_include_directories(corebft PUBLIC include/simplestatetransfer)
target_include_directories(corebft PUBLIC include/metadatastorage)
target_include_directories(corebft PRIVATE src/bftengine)
target_include_directories(corebft PRIVATE src/preprocessor)
target_include_directories(corebft PUBLIC ${perf_include}/performance/include)
target_include_directories(corebft PUBLIC tests/mocks)
target_link_libraries(corebft PUBLIC
  threshsign
  Threads::Threads
  util
  concordbft_storage
  preprocessor
  bftcommunication
  diagnostics
  concordbft_reconfiguration
  secretsmanager
  keys_and_signatures_cmf
  db_checkpoint_msg
  cre
  stdc++fs
  )


target_include_directories(bftclient PUBLIC include/bftengine)
target_include_directories(bftclient PUBLIC src/bftengine)
target_include_directories(bftclient PUBLIC src/preprocessor)
target_include_directories(bftclient PUBLIC ${perf_include})
target_include_directories(bftclient PUBLIC ${kvbc_include})
target_link_libraries(bftclient PUBLIC bftcommunication diagnostics)

install(DIRECTORY include/bftengine DESTINATION include)

if(USE_FAKE_CLOCK_IN_TIME_SERVICE)
    target_compile_definitions(corebft PUBLIC "USE_FAKE_CLOCK_IN_TS=1")
endif()

if(BUILD_SLOWDOWN)
    target_compile_definitions(bftclient PUBLIC USE_SLOWDOWN)
    target_compile_definitions(corebft PUBLIC USE_SLOWDOWN)
endif()
