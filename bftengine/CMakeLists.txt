project (bftengine LANGUAGES CXX)

set(corebft_source_files
    src/bftengine/PrimitiveTypes.cpp
    src/bftengine/PrePrepareMsg.cpp
    src/bftengine/CheckpointMsg.cpp
    src/bftengine/DebugStatistics.cpp
    src/bftengine/Digest.cpp
    src/bftengine/FullCommitProofMsg.cpp
    src/bftengine/FullExecProofMsg.cpp
    src/bftengine/TimeUtils.cpp
    src/bftengine/MessageBase.cpp
    src/bftengine/PartialCommitProofMsg.cpp
    src/bftengine/PartialExecProofMsg.cpp
    src/bftengine/PartialExecProofsSet.cpp
    src/bftengine/PartialProofsSet.cpp
    src/bftengine/SeqNumInfo.cpp
    src/bftengine/SignedShareMsgs.cpp
    src/bftengine/ReplicaImp.cpp
    src/bftengine/ClientReplyMsg.cpp
    src/bftengine/ReqMissingDataMsg.cpp
    src/bftengine/ClientRequestMsg.cpp
    src/bftengine/StartSlowCommitMsg.cpp
    src/bftengine/ControllerBase.cpp
    src/bftengine/ControllerWithSimpleHistory.cpp
    src/bftengine/IncomingMsgsStorage.cpp
    src/bftengine/SimpleAckMsg.cpp
    src/bftengine/RetransmissionsManager.cpp
    src/bftengine/NewViewMsg.cpp
    src/bftengine/SigManager.cpp
    src/bftengine/ReplicasInfo.cpp
    src/bftengine/ViewChangeMsg.cpp
    src/bftengine/ViewChangeSafetyLogic.cpp
    src/bftengine/ViewsManager.cpp
    src/bftengine/CheckpointInfo.cpp
    src/bftengine/ReplicaStatusMsg.cpp
    src/bftengine/StateTransferMsg.cpp
    src/bftengine/ClientsManager.cpp
    src/bftengine/Crypto.cpp
	src/bftengine/NullStateTransfer.cpp
    src/bftengine/BFTEngine.cpp
    src/bftengine/SimpleClient.cpp
    src/bftengine/DebugPersistentStorage.cpp
    src/bftengine/ArchipelagoTimeManager.cpp
    src/bftengine/ArchipelagoStablePointMsg.cpp
    src/bftengine/ArchipelagoSimpleClient.cpp
    src/communication/PlainUDPCommunication.cpp
    src/communication/CommFactory.cpp
	src/bcstatetransfer/BCStateTran.cpp
	src/bcstatetransfer/InMemoryDataStore.cpp
	src/bcstatetransfer/STDigest.cpp
	src/simplestatetransfer/SimpleStateTran.cpp
)
#
# pthread dependency
find_package(Threads REQUIRED)
#message("Threads library: ${CMAKE_THREAD_LIBS_INIT}")

if(${BUILD_COMM_TCP_PLAIN})
    set(corebft_source_files ${corebft_source_files} src/communication/PlainTcpCommunication.cpp)
endif()
if(${BUILD_COMM_TCP_TLS})
    set(corebft_source_files ${corebft_source_files} src/communication/TlsTCPCommunication.cpp)
endif()

#
# Targets
#
add_library(corebft STATIC ${corebft_source_files})

if(${BUILD_COMM_TCP_PLAIN} OR ${BUILD_COMM_TCP_TLS})
    set(Boost_USE_STATIC_LIBS        ON) # only find static libs
    set(Boost_USE_MULTITHREADED      ON)
    set(Boost_USE_STATIC_RUNTIME    OFF)
    find_package(Boost 1.64.0 COMPONENTS system filesystem)
    if(Boost_FOUND)
        include_directories(${Boost_INCLUDE_DIRS})
        target_link_libraries(corebft PUBLIC ${Boost_LIBRARIES})
    endif()
    if(${BUILD_COMM_TCP_PLAIN})
        target_compile_definitions(corebft PUBLIC USE_COMM_PLAIN_TCP)
    elseif(${BUILD_COMM_TCP_TLS})
        find_package(OpenSSL REQUIRED)
        if(OPENSSL_FOUND)
            message("OpenSSL include dir: ${OPENSSL_INCLUDE_DIR}")
            message("OpenSSL libraries: ${OPENSSL_LIBRARIES}")
            target_include_directories(corebft PUBLIC ${OPENSSL_INCLUDE_DIR})
            target_link_libraries(corebft PUBLIC ${OPENSSL_LIBRARIES})
        endif()
        target_compile_definitions(corebft PUBLIC USE_COMM_TLS_TCP)
    endif()

endif()

target_include_directories(corebft PUBLIC include/)
target_include_directories(corebft PUBLIC include/bftengine)
target_include_directories(corebft PUBLIC include/communication)
target_include_directories(corebft PUBLIC include/bcstatetransfer)
target_include_directories(corebft PUBLIC include/simplestatetransfer)
target_include_directories(corebft PUBLIC include/metadatastorage)

target_link_libraries(corebft PUBLIC threshsign)
target_link_libraries(corebft PUBLIC Threads::Threads)
target_link_libraries(corebft PUBLIC util)
