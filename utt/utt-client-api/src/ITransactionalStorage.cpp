// UTT Client API
//
// Copyright (c) 2020-2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#include "storage/ITransactionalStorage.hpp"

namespace utt::client {
bool ITransactionalStorage::isNewStorage() { return storage_->isNewStorage(); }
void ITransactionalStorage::setKeyPair(const std::pair<std::string, std::string>& key_pair) {
  storage_->setKeyPair(key_pair);
}
void ITransactionalStorage::setLastExecutedSn(uint64_t sn) { storage_->setLastExecutedSn(sn); }
void ITransactionalStorage::setClientSideSecret(const libutt::api::types::CurvePoint& s1) {
  storage_->setClientSideSecret(s1);
}
void ITransactionalStorage::setSystemSideSecret(const libutt::api::types::CurvePoint& s2) {
  storage_->setSystemSideSecret(s2);
}
void ITransactionalStorage::setRcmSignature(const libutt::api::types::Signature& sig) {
  storage_->setRcmSignature(sig);
}
void ITransactionalStorage::setCoin(const libutt::api::Coin& coin) { storage_->setCoin(coin); }
void ITransactionalStorage::removeCoin(const libutt::api::Coin& coin) { storage_->removeCoin(coin); }
uint64_t ITransactionalStorage::getLastExecutedSn() { return storage_->getLastExecutedSn(); }
libutt::api::types::CurvePoint ITransactionalStorage::getClientSideSecret() { return storage_->getClientSideSecret(); }
libutt::api::types::CurvePoint ITransactionalStorage::getSystemSideSecret() { return storage_->getSystemSideSecret(); }
libutt::api::types::Signature ITransactionalStorage::getRcmSignature() { return storage_->getRcmSignature(); }
std::vector<libutt::api::Coin> ITransactionalStorage::getCoins() { return storage_->getCoins(); }
std::pair<std::string, std::string> ITransactionalStorage::getKeyPair() { return storage_->getKeyPair(); }
}  // namespace utt::client