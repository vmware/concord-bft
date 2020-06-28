/*
 * Main.cpp
 *
 *  Created on: Jul 11, 2017
 *      Author: atomescu
 */
#include "threshsign/Configuration.h"

#include "Main.h"

#include "XAssert.h"
#include "Logger.hpp"

using std::endl;

int main(int argc, char *argv[]) {
  LOG_ERROR(GL, "Error logging is enabled!");
  LOG_WARN(GL, "Warning logging is enabled!");
  LOG_INFO(GL, "Info logging is enabled!");
  LOG_DEBUG(GL, "Debug logging is enabled!");
  LOG_TRACE(GL, "Trace logging is enabled!");
  LOG_INFO(GL, "");
#ifndef NDEBUG
  LOG_INFO(GL, "Assertions are enabled!");
  assertTrue(true);
  assertFalse(false);
#else
  LOG_INFO(GL, "Assertions are disabled!");
  assertFalse(true);
  assertTrue(false);
#endif
  LOG_DEBUG(GL, "");
  LOG_DEBUG(GL, "Number of arguments: " << argc);

  std::vector<std::string> args;
  for (int i = 0; i < argc; i++) {
    args.push_back(std::string(argv[i]));
  }

  unsigned int seed = static_cast<unsigned int>(time(NULL));
  LOG_INFO(GL, "Randomness seed passed to srand(): " << seed);
  // NOTE: srand is not and should not be used for any cryptographic randomness.
  srand(seed);

  // Call application-defined AppMain()
  int rc = AppMain(args);
  LOG_INFO(GL, "Exited gracefully with rc = " << rc << ".");
  return rc;
}
