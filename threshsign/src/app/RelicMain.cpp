/*
 * RelicMain.cpp
 *
 *  Created on: Aug 4, 2017
 *      Author: alinush
 */

#include "threshsign/Configuration.h"

#include "RelicMain.h"

#include "Log.h"

#include "threshsign/bls/relic/Library.h"

#include <memory>

using BLS::Relic::Library;
using std::endl;

int AppMain(const std::vector<std::string>& args) {
  // Must call this outside TRY {} block
  std::unique_ptr<Library> lib(Library::GetPtr());

  TRY {
    logdbg << "RELIC Type 1 paring: " << pc_map_is_type1() << endl;
    logdbg << "RELIC Type 3 paring: " << pc_map_is_type3() << endl;
    logdbg << "Launching RelicAppMain()..." << endl;

    return RelicAppMain(*lib, args);
  }
  CATCH_ANY {
    logerror << "RELIC threw an exception" << endl;
    // WARNING: For this to work you must build RELIC with CHECK and VERBS
    // defined (see preset/ or my-presets/).
    ERR_PRINT(ERR_CAUGHT);
  }
  FINALLY {}

  return 1;
}
