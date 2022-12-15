// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the
// LICENSE file.

#include "Timer.h"

#include <iostream>

std::ostream& operator<<(std::ostream& out, const AveragingTimer& t) {
  if (t.numIterations() > 0)
    out << t.name << ": " << t.averageLapTime() << " microsec per lap, (" << t.numIterations() << " laps)";
  else
    out << t.name << ": did not run any laps yet.";
  return out;
}
