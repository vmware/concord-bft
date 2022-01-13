cmake_minimum_required (VERSION 3.8)

add_executable(skvbc_integrity main.cpp IntegrityChecker.cpp
    ${concord_bft_tools_SOURCE_DIR}/KeyfileIOUtils.cpp)
set_property(TARGET skvbc_integrity PROPERTY CXX_STANDARD 17)
target_link_libraries(skvbc_integrity PUBLIC test_config_lib corebft kvbc util stdc++fs)

#target_include_directories(skvbc_integrity PUBLIC ..)
target_include_directories(skvbc_integrity PUBLIC ../..)

