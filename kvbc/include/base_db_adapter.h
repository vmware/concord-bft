// Copyright 2020 VMware, all rights reserved
//
// Contains base classes and utilities used to create different DB adapter implementations.

#pragma once

#include "hex_tools.h"
#include "kv_types.hpp"
#include "Logger.hpp"
#include "sliver.hpp"
#include "storage/db_interface.h"
#include <iostream>
#include <utility>
#include <memory>
#include "storage/db_types.h"

namespace concord::kvbc {

class DBAdapterBase {
 protected:
  DBAdapterBase(const std::shared_ptr<storage::IDBClient> db)
      : logger_(concordlogger::Log::getLogger("concord.kvbc.dbadapter")), db_{db} {
    db_->init(false);
  }

 protected:
  concordlogger::Logger logger_;
  std::shared_ptr<storage::IDBClient> db_;
};

}  // namespace concord::kvbc
