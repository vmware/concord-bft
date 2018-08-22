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


#pragma once

#include <bitset>

#include "ThresholdSignaturesTypes.h"

/**
 * TODO: This is kind of inefficient: we store too big of a bitset.
 * We can replace it with boost::dynamic_bitset though.
 *
 * WARNING: This class always receives shares as numbers between 1 and MAX_NUM_OF_SHARES (inclusive!)
 *
 * WARNING: VectorOfShares::add() is not thread-safe because it increments a counter (also not sure if std::bitset is thread-safe either)
 */
class VectorOfShares
{
private:
	// NOTE: data.size() always returns MAX_NUM_OF_SHARES
	std::bitset<MAX_NUM_OF_SHARES> data;
	int size;

	friend std::ostream& operator<<(std::ostream& out, const VectorOfShares& v);

public:
	VectorOfShares() : size(0) {}

public:
	void add(ShareID e);

	void remove(ShareID e);

	bool contains(ShareID e) const;

	int count() const {
	    return size;
	}

	void clear() {
		data.reset();
		size = 0;
	}

	bool isEnd(ShareID e) const;

	ShareID first() const {
		return next(0);
	}

	/**
	 * Returns the ith bit, numbered from 1 to count(), inclusively.
	 */
	ShareID ith(int i) const;

	ShareID last() const;

	ShareID next(ShareID current) const;

	/**
	 * Calls next() count times, starting at 'current'.
	 */
	ShareID skip(ShareID current, int count) const;

	ShareID findFirstGap() const;

	bool operator==(const VectorOfShares& v) const {
		return data == v.data;
	}

	bool operator!=(const VectorOfShares& v) const {
		return data != v.data;
	}

public:
    static void randomSubset(VectorOfShares& signers, int numSigners, int reqSigners);
};

std::ostream& operator<<(std::ostream& out, const VectorOfShares& v);
