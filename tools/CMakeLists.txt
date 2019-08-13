project(concord_bft_tools LANGUAGES CXX)

add_executable(GenerateConcordKeys
               GenerateConcordKeys.cpp
               KeyfileIOUtils.hpp
               KeyfileIOUtils.cpp
               $<TARGET_OBJECTS:logging_dev>)
target_include_directories(GenerateConcordKeys
                           PRIVATE
                           ${threshsign_SOURCE_DIR}/src
                           ${threshsign_SOURCE_DIR}/lib
                           ${threshsign_SOURCE_DIR}/include
                           ${bftengine_SOURCE_DIR}/src
                           ${bftengine_SOURCE_DIR}/include)
target_link_libraries(GenerateConcordKeys
                      PUBLIC
                      threshsign
                      util)
link_with_relic_library(GenerateConcordKeys)
set_target_properties(GenerateConcordKeys
                      PROPERTIES
                      RUNTIME_OUTPUT_DIRECTORY
                      .)
                      
target_compile_options(GenerateConcordKeys PUBLIC "-Wno-extra-semi" "-Wno-undefined-var-template") # TODO tmp cryptopp
add_executable(TestGeneratedKeys
               TestGeneratedKeys.cpp
               KeyfileIOUtils.hpp
               KeyfileIOUtils.cpp
               $<TARGET_OBJECTS:logging_dev>)
target_include_directories(TestGeneratedKeys
                           PRIVATE
                           ${threshsign_SOURCE_DIR}/src
                           ${threshsign_SOURCE_DIR}/lib
                           ${threshsign_SOURCE_DIR}/include
                           ${bftengine_SOURCE_DIR}/src
                           ${bftengine_SOURCE_DIR}/include)
target_link_libraries(TestGeneratedKeys
                      PUBLIC
                      threshsign
                      corebft)
link_with_relic_library(TestGeneratedKeys)
set_target_properties(TestGeneratedKeys
                      PROPERTIES
                      RUNTIME_OUTPUT_DIRECTORY
                      .)

file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/testKeyGeneration.sh
       DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/)
