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

#include "Comparators.h"
#include "Slice.h"
#include "BlockchainDBAdapter.h"
#include "KVBCInterfaces.h"

#include <chrono>

namespace SimpleKVBC {

	int composedKeyComparison(const Slice& _a, const Slice& _b) {
		char aType = extractTypeFromKey(_a);
		char bType = extractTypeFromKey(_b);
		if (aType != bType)
		{
			int ret = aType - bType;
			return ret;
		}

		if (aType == ((char)EDBKeyType::E_DB_KEY_TYPE_BLOCK))
		{
				BlockId aId = extractBlockIdFromKey(_a);
				BlockId bId = extractBlockIdFromKey(_b);
				if (aId > bId) return 1;
				else if (aId == bId) return 0;
				else return (-1);
				//return aId - bId; 
		}
		else
		{
		
			Slice aKey = extractKeyFromKeyComposedWithBlockId(_a);
			Slice bKey = extractKeyFromKeyComposedWithBlockId(_b);

			int keyComp = aKey.compare(bKey);

			if (keyComp == 0)
			{
				BlockId aId = extractBlockIdFromKey(_a);
				BlockId bId = extractBlockIdFromKey(_b);
				if (bId > aId) return 1;
				else if (bId == aId) return 0;
				else return (-1);				
				//return bId - aId; // a < b if keys are equal and bId<aId
			}

			return keyComp;
		}
	}

	/* In memory */
	bool InMemKeyComp(const Slice& _a, const Slice& _b) {
		int comp = composedKeyComparison(_a, _b);
		return comp < 0; // Check: comp < 0 ==> _a < _b
	}

}
