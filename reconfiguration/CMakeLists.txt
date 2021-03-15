find_package(CMFC REQUIRED)
add_subdirectory(cmf)

add_library(concordbft_reconfiguration
        src/reconfiguration_handler.cpp
        src/dispatcher.cpp)
        
target_include_directories(concordbft_reconfiguration PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(concordbft_reconfiguration PUBLIC
 cmf_messages
 corebft
 util
)

