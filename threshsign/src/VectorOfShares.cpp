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

#include "threshsign/VectorOfShares.h"

#include "XAssert.h"

void VectorOfShares::add(ShareID e) {
    assertGreaterThanOrEqual(e, 1);
    assertLessThanOrEqual(e, MAX_NUM_OF_SHARES);

    size_t pos = static_cast<size_t>(e-1);
    if(false == data[pos]) {
        data[pos] = true;
        size++;
    }
}

void VectorOfShares::remove(ShareID e) {
    assertGreaterThanOrEqual(e, 1);
    assertLessThanOrEqual(e, MAX_NUM_OF_SHARES);

    size_t pos = static_cast<size_t>(e-1);
    if(true == data[pos]) {
        data[pos] = false;
        size--;
    }
}

bool VectorOfShares::contains(ShareID e) const {
    size_t pos = static_cast<size_t>(e-1);
    return data[pos];
}

bool VectorOfShares::isEnd(ShareID e) const {
    assertStrictlyGreaterThan(e, 0);
    // Shares are numbered from 1 to MAX_NUM_OF_SHARES and the data bitvector always has size MAX_NUM_OF_SHARES
    // Thus, when e > MAX_NUM_OF_SHARES, we've reached the end.
    // WARNING: Due to C++'s "features", it's important to cast the unsigned type returned by data.size() to a signed type T here!
    return e > static_cast<ShareID>(data.size());
}

ShareID VectorOfShares::next(ShareID current) const
{
    assertGreaterThanOrEqual(current, 0);

    ShareID size = static_cast<ShareID>(data.size());

    // Have we reached the end?
    if(current + 1 > size)
        return size + 1;

    assertStrictlyLessThan(current, MAX_NUM_OF_SHARES);

    for(size_t i = static_cast<size_t>(current); i < data.size(); i++) {
        if(data[i])
            return static_cast<ShareID>(i + 1);
    }

    return size + 1;
}

ShareID VectorOfShares::skip(ShareID current, int count) const {
    assertStrictlyPositive(count);

    for(int i = 0; i < count; i++) {
        current = next(current);
    }

    return current;
}

ShareID VectorOfShares::findFirstGap() const {
    for (size_t i = 0; i < data.size(); i++) {
       if (data[i] == false) {
           return static_cast<ShareID>(i + 1);
       }
    }

    // If all bits are set return size() + 1
    return static_cast<ShareID>(data.size()) + 1;
}

ShareID VectorOfShares::ith(int i) const {
	size_t count = 0;

	// If count is too high
	if(count > data.size())
		return static_cast<ShareID>(data.size()) + 1;

	for(ShareID c = first(); isEnd(c) == false; c = next(c)) {
		count++;
		if(static_cast<size_t>(i) == count)
			return c;
	}

    // Should never reach this
	assertTrue(false);
	return static_cast<ShareID>(data.size()) + 1;
}

ShareID VectorOfShares::last() const {
	size_t size = data.size();
    for (size_t i = 0; i < size; i++) {
       size_t j = size - 1 - i;
       if (data[j] == true) {
           return static_cast<ShareID>(j + 1);
       }
    }

    // If all bits are false return size() + 1
    return static_cast<ShareID>(size) + 1;
}

void VectorOfShares::randomSubset(VectorOfShares& signers, int numSigners, int reqSigners) {
    assertLessThanOrEqual(reqSigners, numSigners);
    // NOTE: Does not need cryptographically secure RNG
    while (signers.count() < reqSigners) {
        ShareID i = static_cast<ShareID>(rand() % numSigners);
        i = i + 1; // WARNING: players need to be indexed from 1 to N, inclusively!

        signers.add(i);
    }
}

std::ostream& operator<<(std::ostream& out, const VectorOfShares& v) {
	out << "[";
	if(v.count() > 0) {
		for(ShareID i = v.first(); v.isEnd(v.next(i)) == false; i = v.next(i)) {
			out << i << ", ";
		}

		out << v.last();
	}
	out << "]";

	return out;
}
