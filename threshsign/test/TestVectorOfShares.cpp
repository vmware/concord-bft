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

#include "threshsign/Configuration.h"

#include <string>
#include <memory>
#include <stdexcept>

#include "Log.h"
#include "XAssert.h"
#include "app/RelicMain.h"

#include "threshsign/VectorOfShares.h"

using namespace std;
using namespace BLS::Relic;

void testIth();

int RelicAppMain(const Library& lib, const std::vector<std::string>& args) {
    (void)lib;
    (void)args;

    testIth();

    return 0;
}

void testIth() {
    VectorOfShares signers;

    signers.add(3);
    if(signers.ith(1) != 3) {
        throw std::logic_error("3 is supposed to be first");
    }

    signers.add(5);
    if(signers.ith(2) != 5) {
        throw std::logic_error("5 is supposed to be second");
    }

    signers.add(7);
    if(signers.skip(3, 1) != signers.next(3)) {
        throw std::logic_error("skip and next disagree");
    }

    if(signers.skip(3, 1) != 5) {
            throw std::logic_error("skip is wrong");
        }
    if(signers.skip(3, 2) != 7) {
        throw std::logic_error("skip is wrong");
    }
}
