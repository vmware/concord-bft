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

#include "DatabaseInterface.h"
#include "KVBCInterfaces.h"
#include "Slice.h"

namespace SimpleKVBC {

	enum class _EDBKeyType
	{
		E_DB_KEY_TYPE_FIRST = 1,
		E_DB_KEY_TYPE_BLOCK = E_DB_KEY_TYPE_FIRST, /* 1 */
		E_DB_KEY_TYPE_KEY, /* 2 */
		E_DB_KEY_TYPE_LAST
	};

	typedef _EDBKeyType EDBKeyType;

	class BlockchainDBAdapter
	{
	public:
		BlockchainDBAdapter(IDBClient* _db) : m_db(_db) {}
		IDBClient* getDb() { return m_db; }

		Status addBlock(BlockId _blockId, Slice _blockRaw);
		Status updateKey(Key _key, BlockId _block, Value _value);
		Status getKeyByReadVersion(BlockId readVersion, Slice key, Slice& outValue, BlockId& outBlock) const;
		Status getBlockById(BlockId _blockId, Slice& _blockRaw, bool& _found) const;
		bool hasBlockId(BlockId _blockId) const;
		Status freeFetchedBlock(Slice& _block) const;

		IDBClient::IDBClientIterator* getIterator() { return m_db->getIterator(); }
		Status freeIterator(IDBClient::IDBClientIterator* _iter) { return m_db->freeIterator(_iter); }
		Status first(IDBClient::IDBClientIterator* iter, BlockId readVersion, OUT BlockId& actualVersion, OUT bool& isEnd, OUT Slice& _key, OUT Slice& _value);
		Status seekAtLeast(IDBClient::IDBClientIterator* iter, Slice _searchKey, BlockId _readVersion, OUT BlockId& _actualVersion, OUT Slice& _key, OUT Slice& _value, OUT bool& _isEnd);
		Status next(IDBClient::IDBClientIterator* iter, BlockId _readVersion, OUT Slice& _key, OUT Slice& _value, OUT BlockId& _actualVersion, OUT bool& _isEnd);

		Status getCurrent(IDBClient::IDBClientIterator* iter, OUT Slice& _key, OUT Slice& _value);
		Status isEnd(IDBClient::IDBClientIterator* iter, OUT bool& _isEnd);

		BlockId getLastBlock();
		BlockId getLastReachableBlock();

	private:
		IDBClient* m_db;
		KeyValuePair m_current;
		bool m_isEnd;

		BlockId lastKnownBlock = 0;
		BlockId lastKnownReachableBlock = 0;

	};

	Slice genDbKey(EDBKeyType _type, Slice _key, BlockId _blockId);
	Slice genBlockDbKey(BlockId _blockId);
	Slice genDataDbKey(Slice _key, BlockId _blockId);
	char extractTypeFromKey(Slice _key);
	BlockId extractBlockIdFromKey(Slice _key);
	Slice extractKeyFromKeyComposedWithBlockId(Slice _composedKey);
}