// Concord
//
// Copyright (c) 2018-2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "st_reconfiguraion_sm.hpp"
#include "hex_tools.h"
#include "endianness.hpp"

namespace concord::kvbc::reconfiguration {
template <typename T>
void StReconfigurationHandler::deserializeCmfMessage(T &msg, const std::string &strval) {
  std::vector<uint8_t> bytesval(strval.begin(), strval.end());
  concord::messages::deserialize(bytesval, msg);
}

uint64_t StReconfigurationHandler::getStoredBftSeqNum(BlockId bid) {
  auto value = ro_storage_.get(kvbc::kConcordInternalCategoryId, std::string{kvbc::keyTypes::bft_seq_num_key}, bid);
  auto sequenceNum = uint64_t{0};
  if (value) {
    const auto &data = std::get<categorization::VersionedValue>(*value).data;
    ConcordAssertEQ(data.size(), sizeof(uint64_t));
    sequenceNum = concordUtils::fromBigEndianBuffer<uint64_t>(data.data());
  }
  return sequenceNum;
}

void StReconfigurationHandler::stCallBack(uint64_t) {
  concord::messages::ReconfigurationErrorMsg error_msg;
  // Handle reconfiguration state changes if exist
  handlerStoredCommand<concord::messages::WedgeCommand>(std::string{kvbc::keyTypes::reconfiguration_wedge_key},
                                                        error_msg);
  handlerStoredCommand<concord::messages::DownloadCommand>(std::string{kvbc::keyTypes::reconfiguration_download_key},
                                                           error_msg);
  handlerStoredCommand<concord::messages::InstallCommand>(std::string{kvbc::keyTypes::reconfiguration_install_key},
                                                          error_msg);
  handlerStoredCommand<concord::messages::KeyExchangeCommand>(std::string{kvbc::keyTypes::reconfiguration_key_exchange},
                                                              error_msg);
  handlerStoredCommand<concord::messages::AddRemoveCommand>(std::string{kvbc::keyTypes::reconfiguration_add_remove},
                                                            error_msg);
}
template <typename T>
bool StReconfigurationHandler::handlerStoredCommand(const std::string &key,
                                                    concord::messages::ReconfigurationErrorMsg &error_msg) {
  auto res = ro_storage_.getLatest(kvbc::kConcordInternalCategoryId, key);
  if (res.has_value()) {
    auto blockid =
        ro_storage_
            .getLatestVersion(kvbc::kConcordInternalCategoryId, std::string{kvbc::keyTypes::reconfiguration_wedge_key})
            .value()
            .version;
    auto seqNum = getStoredBftSeqNum(blockid);
    auto strval = std::visit([](auto &&arg) { return arg.data; }, *res);
    T cmd;
    deserializeCmfMessage(cmd, strval);
    return handle(cmd, seqNum, error_msg);
  }
  return false;
}

}  // namespace concord::kvbc::reconfiguration