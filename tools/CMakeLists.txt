project(concord_bft_tools LANGUAGES CXX)

add_executable(GenerateConcordKeys
               GenerateConcordKeys.cpp
               KeyfileIOUtils.hpp
               KeyfileIOUtils.cpp
               )

if(BUILD_THIRDPARTY)
    add_dependencies(GenerateConcordKeys cryptopp)
else()
    find_package(cryptopp REQUIRED)
endif()

target_include_directories(GenerateConcordKeys
                           PRIVATE
                           ${threshsign_SOURCE_DIR}/src
                           ${threshsign_SOURCE_DIR}/lib
                           ${threshsign_SOURCE_DIR}/include
                           ${bftengine_SOURCE_DIR}/src
                           ${bftengine_SOURCE_DIR}/include
                           ${CRYPTOPP_INCLUDE_DIRS})

target_link_libraries(GenerateConcordKeys
                      PUBLIC
                      threshsign
                      ${CRYPTOPP_LIBRARIES}
                      util
					  stdc++fs)

set_target_properties(GenerateConcordKeys
                      PROPERTIES
                      RUNTIME_OUTPUT_DIRECTORY
                      .)

if (BUILD_TESTING)
	add_executable(TestGeneratedKeys
	               TestGeneratedKeys.cpp
	               KeyfileIOUtils.hpp
	               KeyfileIOUtils.cpp
	               )

	target_include_directories(TestGeneratedKeys
			PRIVATE
			${threshsign_SOURCE_DIR}/src
			${threshsign_SOURCE_DIR}/lib
			${threshsign_SOURCE_DIR}/include
			${bftengine_SOURCE_DIR}/src
			${bftengine_SOURCE_DIR}/src/bftengine/include_internal
			${bftengine_SOURCE_DIR}/include)

	target_link_libraries(TestGeneratedKeys
			PUBLIC
			threshsign
			corebft)
	set_target_properties(TestGeneratedKeys
			PROPERTIES
			RUNTIME_OUTPUT_DIRECTORY
			.)
endif()

if (BUILD_ROCKSDB_STORAGE)
    add_executable(skvb_db_editor DBEditor.cpp )
    target_include_directories(skvb_db_editor PUBLIC
        ${PROJECT_SOURCE_DIR}/src include)
    target_link_libraries(skvb_db_editor kvbc corebft concordbft_storage)
endif(BUILD_ROCKSDB_STORAGE)

file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/testKeyGeneration.sh
       DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/)
