// Concord
//
// Copyright (c) 2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#ifndef CONCORD_KVBC_KEY_TYPES_H_
#define CONCORD_KVBC_KEY_TYPES_H_

namespace concord {
namespace kvbc {

// Eth 0x00 - 0x0f
const char kKvbKeyEthBlock = 0x01;
const char kKvbKeyEthTransaction = 0x02;
const char kKvbKeyEthBalance = 0x03;
const char kKvbKeyEthCode = 0x04;
const char kKvbKeyEthStorage = 0x05;
const char kKvbKeyEthNonce = 0x06;
const char kKvbKeyEthBlockHash = 0x07;

// Unused 0x10 - 0x1f

// Concord 0x20 - 0x2f
const char kKvbKeyTimeSamples = 0x20;
const char kKvbKeyMetadata = 0x21;
const char kKvbKeySummarizedTime = 0x22;
const char kKvbKeyCorrelationId = 0x23;
const char kKvbKeyLastAgreedPrunableBlockId = 0x24;
const char kIndexKey = 0x25;
const char kClientsPublicKeys = 0x2b;

// DAML 0x30 - 0x3f
// const char kKvbKeyDaml = 0x30;
const char kKvbKeyAdminIdentifier = 0x40;

}  // namespace kvbc
}  // namespace concord

#endif  // CONCORD_KVBC_KEY_TYPES_H_
