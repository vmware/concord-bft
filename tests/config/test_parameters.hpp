// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

struct ClientParams {
  uint32_t numOfOperations = 4600;
  uint16_t clientId = 4;
  uint16_t numOfReplicas = 4;
  uint16_t numOfClients = 1;
  uint16_t numOfFaulty = 1;
  uint16_t numOfSlow = 0;
  std::string configFileName;
  bool measurePerformance = false;

  uint16_t get_numOfReplicas() { return (uint16_t)(3 * numOfFaulty + 2 * numOfSlow + 1); }
};

enum class PersistencyMode {
  Off,       // no persistency at all
  InMemory,  // use in memory module
  File,      // use file as a storage
  RocksDB,   // use RocksDB for storage
  MAX_VALUE = RocksDB
};

enum class ReplicaBehavior {
  Default,
  Replica0OneTimeRestartVC,
  Replica2PeriodicRestartNoVC,
  AllReplicasRandomRestartNoVC,
  AllReplicasRandomRestartVC,
  MAX_VALUE = AllReplicasRandomRestartVC
};

struct ReplicaParams {
  uint16_t replicaId;
  uint16_t numOfReplicas = 4;
  uint16_t numOfClients = 1;
  bool debug = false;
  bool viewChangeEnabled = false;
  bool autoPrimaryRotationEnabled = false;
  uint16_t viewChangeTimeout = 60000;              // ms
  uint16_t autoPrimaryRotationTimeout = 40000;     // ms
  uint16_t statusReportTimerMillisec = 10 * 1000;  // ms
  std::string configFileName;
  std::string keysFilePrefix;
  PersistencyMode persistencyMode = PersistencyMode::Off;
  ReplicaBehavior replicaBehavior = ReplicaBehavior::Default;
};
