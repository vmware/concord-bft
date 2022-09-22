find_package(GTest REQUIRED)
add_executable(sig_processor_tests sigProcessorTests.cpp )
add_test(sig_processor_tests sig_processor_tests)
target_link_libraries(sig_processor_tests PRIVATE 
                                        logging 
                                        GTest::Main 
                                        GTest::GTest 
                                        sig_processor)
