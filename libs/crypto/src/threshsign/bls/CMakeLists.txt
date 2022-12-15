add_subdirectory(relic)

add_library(threshsign_bls STATIC XAssert.cpp
                                  Utils.cpp
                                  Timer.cpp
                                  IPublicParameters.cpp
                                  VectorOfShares.cpp
                                  )
target_include_directories(threshsign_bls PUBLIC ${PROJECT_SOURCE_DIR})
target_link_libraries(threshsign PUBLIC threshsign_bls)