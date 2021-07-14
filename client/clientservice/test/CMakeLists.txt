find_package(GTest REQUIRED)

# TODO: Why do we need to specifically add relic here?!
# Without we get a linker error for undefined symbol BLS::Relic::Library::Library()
add_relic_executable(clientservice-test-yaml_parsing yaml_parsing.cpp .)
target_link_libraries(clientservice-test-yaml_parsing PUBLIC
  GTest::Main
  clientservice-lib
)
add_test(clientservice-test-yaml_parsing clientservice-test-yaml_parsing)
