get_property(LOGGER_INC_DIR GLOBAL PROPERTY LoggerIncludeDir)

set(corebft_source_files
    src/bftengine/PrePrepareMsg.cpp
    src/bftengine/CheckpointMsg.cpp
    src/bftengine/DebugStatistics.cpp
    src/bftengine/Digest.cpp
    src/bftengine/FullCommitProofMsg.cpp
    src/bftengine/FullExecProofMsg.cpp
    src/bftengine/TimeUtils.cpp
    src/bftengine/Logger.cpp
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
    src/bftengine/SimpleThreadPool.cpp
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
    src/communication/PlainUDPCommunication.cpp
    src/communication/CommFactory.cpp
    ${LOGGER_INC_DIR}/Logging.hpp
)
#
# pthread dependency
find_package(Threads REQUIRED)
#message("Threads library: ${CMAKE_THREAD_LIBS_INIT}")

if(${BUILD_COMM_TCP_PLAIN})
    set(corebft_source_files ${corebft_source_files} src/communication/PlainTcpCommunication.cpp src/communication/CommFactory.cpp)
endif()

#
# Targets
#
add_library(corebft STATIC ${corebft_source_files})

if(${BUILD_COMM_TCP_PLAIN})
    set(Boost_USE_STATIC_LIBS        ON) # only find static libs
    set(Boost_USE_MULTITHREADED      ON)
    set(Boost_USE_STATIC_RUNTIME    OFF)
    find_package(Boost 1.64.0 COMPONENTS system)
    if(Boost_FOUND)
        include_directories(${Boost_INCLUDE_DIRS})
        target_link_libraries(corebft PUBLIC ${Boost_LIBRARIES})
    endif()
    target_compile_definitions(corebft PUBLIC USE_COMM_PLAIN_TCP)
endif()

TARGET_COMPILE_DEFINITIONS(corebft PUBLIC
        DEFAULT_LOGGER_NAME="${CONCORD_LOGGER_NAME}")
if(${USE_LOG4CPP})
    TARGET_COMPILE_DEFINITIONS(corebft PUBLIC USE_LOG4CPP)
    target_link_libraries(corebft PUBLIC log4cplus)
endif()

target_include_directories(corebft PUBLIC include/bftengine)
target_include_directories(corebft PUBLIC include/communication)
target_include_directories(corebft PUBLIC ${LOGGER_INC_DIR})

target_link_libraries(corebft PUBLIC threshsign)
target_link_libraries(corebft PUBLIC Threads::Threads)

add_subdirectory(tests)