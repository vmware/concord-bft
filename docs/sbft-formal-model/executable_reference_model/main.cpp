#include "replica.h"
#include <iostream>

int main(int argc, char* argv[])
{
  auto conf = ClusterConfig::Constants{2, 1};
  Replica::Constants c{3, conf};
  Replica::Variables v{0};
  return 0;
}