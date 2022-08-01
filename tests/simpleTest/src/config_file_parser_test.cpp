#include "config_file_parser.hpp"
#include "assertUtils.hpp"

#include <iostream>
#include <string>

using std::cout;
using std::cin;
using std::string;
using std::vector;

int main(int argc, char** argv) {
  cout << "Enter configuration file name with a full/relative path,"
       << " or 'd' for a default:\n";

  const string default_config_file = "scripts/sample_config.txt";
  string config_file = default_config_file;
  string given_config_file;
  const string use_default_config_file = "d";
  const uint expected_replicas_num = 4;
  const uint expected_clients_num = 1;
  const string expected_replica1 = "127.0.0.1:3410";
  const string expected_replica2 = "127.0.0.1:3420";
  const string expected_replica3 = "127.0.0.1:3430";
  const string expected_replica4 = "127.0.0.1:3440";
  const string expected_client = "127.0.0.1:4444";
  const string values_to_split = "10.23.43.1:1234:1238";
  const string expected_split_values[] = {"10.23.43.1", "1234", "1238"};
  const string values_to_split_delimiter = ":";

  cin >> given_config_file;
  if (given_config_file != use_default_config_file) config_file = given_config_file;
  logging::Logger logger = logging::getLogger("simpletest.test");
  concord::util::ConfigFileParser parser(logger, config_file);

  try {
    parser.parse();
  } catch (const std::exception& e) {
    cout << e.what() << "\n";
    return 1;
  }
  cout << "\n";
  size_t replicas_num = parser.count("replicas_config");
  auto replicas = parser.get_values<std::string>("replicas_config");

  size_t clients_num = parser.count("clients_config");
  auto clients = parser.get_values<std::string>("clients_config");
  parser.printAll();

  vector<std::string> split_values_vector = parser.splitValue(values_to_split, values_to_split_delimiter.c_str());

  if (config_file == default_config_file) {
    ConcordAssert(replicas_num == expected_replicas_num);
    ConcordAssert(clients_num == expected_clients_num);
    ConcordAssert(expected_replica1 == replicas[0]);
    ConcordAssert(expected_replica2 == replicas[1]);
    ConcordAssert(expected_replica3 == replicas[2]);
    ConcordAssert(expected_replica4 == replicas[3]);
    ConcordAssert(expected_client == clients[0]);
    ConcordAssert(split_values_vector.size() == sizeof(expected_split_values) / sizeof(expected_split_values[0]));
    ConcordAssert(split_values_vector[0] == expected_split_values[0]);
    ConcordAssert(split_values_vector[1] == expected_split_values[1]);
    ConcordAssert(split_values_vector[2] == expected_split_values[2]);
  }
  return 0;
}
