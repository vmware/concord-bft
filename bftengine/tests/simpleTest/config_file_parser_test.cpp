#include "config_file_parser.hpp"

#include <iostream>
#include <string>

using std::cout;
using std::cin;
using std::string;

int main(int argc, char **argv) {

  cout << "Enter configuration file name\n";

  string tmp, file = "config.txt";
  cin >> tmp;
  if (tmp == ".\n")
    file = tmp;
  concordlogger::Logger logger =
      concordlogger::Logger::getLogger("simpletest.test");
  ConfigFileParser parser(logger, file);
  if (!parser.Parse())
    return 1;
  cout << "\n";
  parser.Count("replicas_config");
  parser.GetValues("replicas_config");

  parser.Count("clients_config");
  parser.GetValues("clients_config");
  parser.printAll();

  parser.SplitValue("10.23.43.1:1234:1238", ":");
  return 0;
}
