if (USE_S3_OBJECT_STORE)

project(object_store_utility LANGUAGES CXX )
find_package(Boost ${MIN_BOOST_VERSION} COMPONENTS program_options REQUIRED)

add_library(libobject_store integrity_checker.cpp db_restore.cpp)
target_link_libraries(libobject_store PUBLIC kvbc util Boost::program_options)

add_executable(object_store_utility main.cpp) 
set_property(TARGET object_store_utility PROPERTY CXX_STANDARD 17)
target_link_libraries(object_store_utility PRIVATE libobject_store)

configure_file(logging.properties ${PROJECT_BINARY_DIR} COPYONLY)

endif(USE_S3_OBJECT_STORE)
