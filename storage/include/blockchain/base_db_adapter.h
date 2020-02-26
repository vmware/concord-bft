// Copyright 2020 VMware, all rights reserved
//
// Contains base classes and utilities used to create different DB adapter implementations.

#pragma once

#include "hex_tools.h"
#include "kv_types.hpp"
#include "Logger.hpp"
#include "sliver.hpp"
#include "storage/db_interface.h"
#include "blockchain/db_types.h"

#include <iostream>
#include <utility>
#include <memory>

namespace concord {
namespace storage {
namespace blockchain {

struct HexPrintBuffer {
  const char *bytes;
  const size_t size;
};

// Print a char* of bytes as its 0x<hex> representation.
inline std::ostream &operator<<(std::ostream &s, const HexPrintBuffer p) {
  concordUtils::hexPrint(s, p.bytes, p.size);
  return s;
}

class DBKeyManipulatorBase {
 protected:
  static concordlogger::Logger &logger() {
    static concordlogger::Logger logger_ = concordlogger::Log::getLogger("concord.storage.blockchain.DBKeyManipulator");
    return logger_;
  }
};

class DBAdapterBase {
 protected:
  DBAdapterBase(const std::shared_ptr<IDBClient> &db, bool readOnly);

 public:
  std::shared_ptr<IDBClient> getDb() const { return db_; }

  IDBClient::IDBClientIterator *getIterator() { return db_->getIterator(); }

  Status freeIterator(IDBClient::IDBClientIterator *_iter) { return db_->freeIterator(_iter); }

  // Used to monitor the DB.
  void monitor() const { db_->monitor(); }

 protected:
  concordlogger::Logger logger_;
  std::shared_ptr<IDBClient> db_;
  KeyValuePair m_current;
  bool m_isEnd{false};
};

}  // namespace blockchain
}  // namespace storage
}  // namespace concord
