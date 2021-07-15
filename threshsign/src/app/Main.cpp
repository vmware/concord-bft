/*
 * Main.cpp
 *
 *  Created on: Jul 11, 2017
 *      Author: atomescu
 */

#include "Main.h"

#include "XAssert.h"
#include "Logger.hpp"

using std::endl;

int main(int argc, char *argv[]) {
  logging::initLogger("logging.properties");

  LOG_ERROR(THRESHSIGN_LOG, "Error logging is enabled!");
  LOG_WARN(THRESHSIGN_LOG, "Warning logging is enabled!");
  LOG_INFO(THRESHSIGN_LOG, "Info logging is enabled!");
  LOG_DEBUG(THRESHSIGN_LOG, "Debug logging is enabled!");
  LOG_TRACE(THRESHSIGN_LOG, "Trace logging is enabled!");
  LOG_INFO(THRESHSIGN_LOG, "");
#ifndef NDEBUG
  LOG_INFO(THRESHSIGN_LOG, "Assertions are enabled!");
  assertTrue(true);
  assertFalse(false);
#else
  LOG_INFO(THRESHSIGN_LOG, "Assertions are disabled!");
  assertFalse(true);
  assertTrue(false);
#endif
  LOG_DEBUG(THRESHSIGN_LOG, "");
  LOG_DEBUG(THRESHSIGN_LOG, "Number of arguments: " << argc);

  std::vector<std::string> args;
  args.reserve(static_cast<size_t>(argc));
  for (int i = 0; i < argc; i++) {
    args.push_back(std::string(argv[i]));
  }

  unsigned int seed = static_cast<unsigned int>(time(NULL));
  LOG_INFO(THRESHSIGN_LOG, "Randomness seed passed to srand(): " << seed);
  // NOTE: srand is not and should not be used for any cryptographic randomness.
  srand(seed);

  // Call application-defined AppMain()
  int rc = AppMain(args);
  LOG_INFO(THRESHSIGN_LOG, "Exited gracefully with rc = " << rc << ".");
  return rc;
}
