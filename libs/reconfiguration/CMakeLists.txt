project (libreconfiguration LANGUAGES CXX)
add_subdirectory(cmf)
        
add_library(concordbft_reconfiguration
        src/dispatcher.cpp
        src/reconfiguration.cpp)

target_link_libraries(concordbft_reconfiguration PUBLIC cmf_messages concord-crypto)

target_include_directories(concordbft_reconfiguration PUBLIC .)

target_sources(concordbft_reconfiguration PUBLIC FILE_SET reconfiguration_pub_hdrs
                                       TYPE HEADERS
                                       FILES 
                                       dispatcher.hpp
                                       ireconfiguration.hpp
                                       reconfiguration.hpp)

install(TARGETS concordbft_reconfiguration FILE_SET reconfiguration_pub_hdrs DESTINATION reconfiguration)

