project (bftengine LANGUAGES CXX)

add_subdirectory(src/preprocessor)

set(corebft_source_files
    src/bftengine/PrimitiveTypes.cpp
    src/bftengine/DebugStatistics.cpp
    src/bftengine/Digest.cpp
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
    src/bftengine/CheckpointInfo.cpp
    src/bftengine/ClientsManager.cpp
    src/bftengine/Crypto.cpp
    src/bftengine/NullStateTransfer.cpp
    src/bftengine/BFTEngine.cpp
    src/bftengine/SimpleClientImp.cpp
    src/bftengine/PersistentStorageImp.cpp
    src/bftengine/PersistentStorageDescriptors.cpp
    src/bftengine/PersistentStorageWindows.cpp
    src/bftengine/DebugPersistentStorage.cpp
    src/bftengine/ReplicaLoader.cpp
    src/bftengine/ReplicaConfigSerializer.cpp
    src/bftengine/SerializableActiveWindow.cpp
    src/bftengine/MsgsCommunicator.cpp
    src/bftengine/MsgReceiver.cpp
    src/bftengine/DbMetadataStorage.cpp
    src/bcstatetransfer/BCStateTran.cpp
    src/bcstatetransfer/InMemoryDataStore.cpp
    src/bcstatetransfer/STDigest.cpp
    src/bcstatetransfer/DBDataStore.cpp
    src/bcstatetransfer/SourceSelector.cpp
    src/simplestatetransfer/SimpleStateTran.cpp
    src/bftengine/messages/PrePrepareMsg.cpp
    src/bftengine/messages/CheckpointMsg.cpp
    src/bftengine/messages/FullCommitProofMsg.cpp
    src/bftengine/messages/FullExecProofMsg.cpp
    src/bftengine/messages/MessageBase.cpp
    src/bftengine/messages/PartialCommitProofMsg.cpp
    src/bftengine/messages/PartialExecProofMsg.cpp
    src/bftengine/messages/PartialExecProofsSet.cpp
    src/bftengine/messages/PartialProofsSet.cpp
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
    src/bftengine/messages/InternalMessage.cpp
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

find_package(cryptopp REQUIRED)
find_package(OpenTracing REQUIRED)
find_package(Boost REQUIRED)
find_package(yaml-cpp REQUIRED)
find_package(thrift REQUIRED)
find_package(jaegertracing REQUIRED)

target_compile_options(corebft PUBLIC "-Wno-extra-semi" "-Wno-undefined-var-template") # TODO tmp cryptopp
target_include_directories(corebft PUBLIC include/)
target_include_directories(corebft PUBLIC include/bftengine)
target_include_directories(corebft PUBLIC include/bcstatetransfer)
target_include_directories(corebft PUBLIC include/simplestatetransfer)
target_include_directories(corebft PUBLIC include/metadatastorage)
target_include_directories(corebft PRIVATE src/bftengine)
target_include_directories(corebft PRIVATE src/preprocessor)
target_include_directories(corebft PRIVATE util/include)
target_include_directories(corebft PUBLIC ${CRYPTOPP_INCLUDE_DIRS})
target_include_directories(corebft PUBLIC ${OPENTRACING_INCLUDE_DIRS})
target_include_directories(corebft PUBLIC ${YAML-CPP_INCLUDE_DIRS})
target_include_directories(corebft PUBLIC ${JAEGERTRACING_INCLUDE_DIRS})

target_include_directories(bftclient PUBLIC include/bftengine)
target_include_directories(bftclient PUBLIC src/bftengine)
target_include_directories(bftclient PUBLIC src/preprocessor)
target_link_libraries(corebft PUBLIC
  threshsign
  Threads::Threads
  util
  concordbft_storage
  preprocessor
  bftcommunication
)
target_link_libraries(bftclient PUBLIC bftcommunication)
target_link_libraries(corebft PUBLIC opentracing)
target_link_libraries(corebft PUBLIC ${THRIFT_LIBRARIES})
target_link_libraries(corebft PUBLIC ${YAML-CPP_LIBRARIES})
target_link_libraries(corebft PUBLIC ${JAEGERTRACING_LIBRARIES})

message("BOOST INC DIRS:" ${Boost_INCLUDE_DIRS})
