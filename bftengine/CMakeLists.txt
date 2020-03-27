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
    src/communication/PlainUDPCommunication.cpp
    src/communication/CommFactory.cpp
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

if(${BUILD_COMM_TCP_PLAIN})
    set(corebft_source_files ${corebft_source_files} src/communication/PlainTcpCommunication.cpp)
endif()
if(${BUILD_COMM_TCP_TLS})
    set(corebft_source_files ${corebft_source_files} src/communication/TlsTCPCommunication.cpp tests/testMsgsCertificate/msgsCertificate_test.cpp)
endif()

#
# Targets
#
add_library(corebft STATIC ${corebft_source_files})
add_library(bftclient STATIC src/bftengine/SimpleClientImp)

if(${BUILD_COMM_TCP_PLAIN} OR ${BUILD_COMM_TCP_TLS})
    set(Boost_USE_STATIC_LIBS ON) # only find static libs
    set(Boost_USE_MULTITHREADED ON)
    set(Boost_USE_STATIC_RUNTIME OFF)
    find_package(Boost ${MIN_BOOST_VERSION} COMPONENTS system filesystem REQUIRED)
    include_directories(${Boost_INCLUDE_DIRS})
    target_link_libraries(corebft PUBLIC ${Boost_LIBRARIES})
    target_link_libraries(bftclient PUBLIC ${Boost_LIBRARIES})
    if(${BUILD_COMM_TCP_PLAIN})
        target_compile_definitions(corebft PUBLIC USE_COMM_PLAIN_TCP)
        target_compile_definitions(bftclient PUBLIC USE_COMM_PLAIN_TCP)
    elseif(${BUILD_COMM_TCP_TLS})
        find_package(OpenSSL REQUIRED)
        target_include_directories(corebft PUBLIC ${OPENSSL_INCLUDE_DIR})
        target_link_libraries(corebft PUBLIC ${OPENSSL_LIBRARIES})
        target_link_libraries(bftclient PUBLIC ${OPENSSL_LIBRARIES})
        target_compile_definitions(corebft PUBLIC USE_COMM_TLS_TCP)
        target_compile_definitions(bftclient PUBLIC USE_COMM_TLS_TCP)
    endif()
endif()
find_package(cryptopp REQUIRED)

target_compile_options(corebft PUBLIC "-Wno-extra-semi" "-Wno-undefined-var-template") # TODO tmp cryptopp
target_include_directories(corebft PUBLIC include/)
target_include_directories(corebft PUBLIC include/bftengine)
target_include_directories(corebft PUBLIC include/communication)
target_include_directories(corebft PUBLIC include/bcstatetransfer)
target_include_directories(corebft PUBLIC include/simplestatetransfer)
target_include_directories(corebft PUBLIC include/metadatastorage)
target_include_directories(corebft PRIVATE src/bftengine)
target_include_directories(corebft PRIVATE src/preprocessor)
target_include_directories(corebft PRIVATE util/include)
target_include_directories(corebft PUBLIC ${CRYPTOPP_INCLUDE_DIRS})

target_include_directories(bftclient PUBLIC include/bftengine)
target_include_directories(bftclient PUBLIC include/communication)
target_include_directories(bftclient PUBLIC src/bftengine)
target_include_directories(bftclient PUBLIC src/preprocessor)

target_link_libraries(corebft PUBLIC threshsign)
target_link_libraries(corebft PUBLIC Threads::Threads)
target_link_libraries(corebft PUBLIC util)
target_link_libraries(corebft PUBLIC concordbft_storage)
target_link_libraries(corebft PUBLIC preprocessor)
