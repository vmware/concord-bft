/*
 * RelicMain.cpp
 *
 *  Created on: Aug 4, 2017
 *      Author: alinush
 */

#include "threshsign/Configuration.h"

#include "RelicMain.h"

#include "Logger.hpp"

#include "threshsign/bls/relic/Library.h"

#include <memory>

using BLS::Relic::Library;

int AppMain(const std::vector<std::string>& args) {
  // Must call this outside TRY {} block
  std::unique_ptr<Library> lib(Library::GetPtr());

  TRY {
    LOG_DEBUG(THRESHSIGN_LOG, "RELIC Type 1 paring: " << pc_map_is_type1());
    LOG_DEBUG(THRESHSIGN_LOG, "RELIC Type 3 paring: " << pc_map_is_type3());
    LOG_DEBUG(THRESHSIGN_LOG, "Launching RelicAppMain()...");

    return RelicAppMain(*lib, args);
  }
  CATCH_ANY {
    LOG_ERROR(THRESHSIGN_LOG, "RELIC threw an exception");
    // WARNING: For this to work you must build RELIC with CHECK and VERBS defined (see preset/ or my-presets/).
    ERR_PRINT(ERR_CAUGHT);
  }
  FINALLY {}

  return 1;
}
