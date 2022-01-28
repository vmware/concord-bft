add_subdirectory(test)


add_library(concord_resources_manager STATIC
  src/IntervalMappingResourceManager.cpp
  src/AdaptivePruningManager.cpp
 )

target_include_directories(concord_resources_manager PUBLIC include)

target_include_directories(concord_resources_manager PUBLIC
  include
)

target_link_libraries(concord_resources_manager PUBLIC
  concordbft_reconfiguration
  bftcommunication
)
