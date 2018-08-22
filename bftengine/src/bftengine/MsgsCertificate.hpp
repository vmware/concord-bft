//Concord
//
//Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
//This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in compliance with the Apache 2.0 License. 
//
//This product may include a number of subcomponents with separate copyright notices and license terms. Your use of these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE file.

#pragma once

#include <cstring>
#include <stdint.h>
#include <unordered_map>
#include <forward_list> 

// TODO(GG): write a simpler version (we don't really need all features of this class)

namespace bftEngine
{
	namespace impl
	{

		template <typename T,
			bool SelfTrust,		  // = true, 
			bool SelfIsRequired,   // = false, 
			bool KeepAllMsgs, 	  // = true, 	
			typename ExternalFunc
		>
			class MsgsCertificate
		{
		public:
			MsgsCertificate(const uint16_t numOfReplicas, const uint16_t maxFailures,
				const uint16_t numOfRequired, const ReplicaId selfReplicaId);

			~MsgsCertificate();

			bool addMsg(T* msg, ReplicaId replicaId);

			bool isComplete() const;

			bool isInconsistent() const;

			T* selfMsg() const;

			T* bestCorrectMsg() const;

			void tryToMarkComplete();

			bool hasMsgFromReplica(ReplicaId replicaId) const;

			void resetAndFree();

			bool isEmpty() const;

			// for debug

			std::forward_list<ReplicaId> includedReplicas() const;

		protected:

			void addPeerMsg(T* msg, ReplicaId replicaId);
			void addSelfMsg(T* msg);

			static const uint16_t NULL_CLASS = UINT16_MAX;

			const uint16_t numOfReps;
			const uint16_t maxFails;
			const uint16_t required;
			const ReplicaId selfId;

			std::unordered_map<ReplicaId, T*> msgsFromReplicas;

			struct MsgClassInfo {
				uint16_t size;
				uint16_t representativeReplica;
			};

			MsgClassInfo* msgClasses = nullptr;
			uint16_t numOfClasses = 0;

			bool complete = false;

			uint16_t bestClass = NULL_CLASS;
			uint16_t sizeOfBestClass = 0;

			bool hasTrustedSelfClass = false;
		};


		template <typename T, bool SelfTrust, bool SelfIsRequired, bool KeepAllMsgs, typename ExternalFunc>
		MsgsCertificate<T, SelfTrust, SelfIsRequired, KeepAllMsgs, ExternalFunc>::MsgsCertificate(const uint16_t numOfReplicas, const uint16_t maxFailures,
			const uint16_t numOfRequired, const ReplicaId selfReplicaId)
			: numOfReps{ numOfReplicas }, maxFails{ maxFailures }, required{ numOfRequired },
			selfId(selfReplicaId)
		{
			static_assert(KeepAllMsgs, "KeepAllMsgs==false is not supported yet");
			static_assert(!SelfIsRequired || SelfTrust, "SelfIsRequired=true requires SelfTrust=true");

			// TODO(GG): more asserts

			msgClasses = new MsgClassInfo[numOfReps];
			memset(msgClasses, 0, numOfReps * sizeof(MsgClassInfo));
		}

		template <typename T, bool SelfTrust, bool SelfIsRequired, bool KeepAllMsgs, typename ExternalFunc>
		MsgsCertificate<T, SelfTrust, SelfIsRequired, KeepAllMsgs, ExternalFunc>::~MsgsCertificate()
		{
			resetAndFree();
			delete[] msgClasses;
		}

		template <typename T, bool SelfTrust, bool SelfIsRequired, bool KeepAllMsgs, typename ExternalFunc>
		bool MsgsCertificate<T, SelfTrust, SelfIsRequired, KeepAllMsgs, ExternalFunc>::isEmpty() const
		{
			return (msgsFromReplicas.empty());
		}

		template <typename T, bool SelfTrust, bool SelfIsRequired, bool KeepAllMsgs, typename ExternalFunc>
		void MsgsCertificate<T, SelfTrust, SelfIsRequired, KeepAllMsgs, ExternalFunc>::resetAndFree()
		{
			complete = false;

			if (msgsFromReplicas.empty())
				return; // nothing to do 

			for (auto&& m : msgsFromReplicas)
				delete m.second;

			msgsFromReplicas.clear();

			if (numOfClasses > 0) memset(msgClasses, 0, numOfClasses * sizeof(MsgClassInfo));
			numOfClasses = 0;

			bestClass = NULL_CLASS;
			sizeOfBestClass = 0;
			hasTrustedSelfClass = false;
		}



		template <typename T, bool SelfTrust, bool SelfIsRequired, bool KeepAllMsgs, typename ExternalFunc>
		bool MsgsCertificate<T, SelfTrust, SelfIsRequired, KeepAllMsgs, ExternalFunc>::addMsg(T* msg, ReplicaId replicaId)
		{
			if (msgsFromReplicas.count(replicaId) > 0) {
				delete msg;
				return false;
			}

			msgsFromReplicas[replicaId] = msg;

			if (!complete)
			{
				if (SelfTrust)
				{
					if (replicaId == selfId)
						addSelfMsg(msg);
					else
						addPeerMsg(msg, replicaId);
				}
				else
				{
					addPeerMsg(msg, replicaId);
				}
			}

			return true;
		}

		template <typename T, bool SelfTrust, bool SelfIsRequired, bool KeepAllMsgs, typename ExternalFunc>
		void MsgsCertificate<T, SelfTrust, SelfIsRequired, KeepAllMsgs, ExternalFunc>::addPeerMsg(T* msg, ReplicaId replicaId)
		{
			uint16_t relevantClass = NULL_CLASS;

			if (hasTrustedSelfClass)
			{
				MsgClassInfo& cls = msgClasses[0]; // in this case, we have a single class

				auto pos = msgsFromReplicas.find(cls.representativeReplica);
				// TODO Assert(pos is okay)
				T* representativeMsg = pos->second;

				if (!ExternalFunc::equivalent(representativeMsg, msg))
					return; // msg should be ignored

				relevantClass = 0;
				cls.size = cls.size + 1;
			}
			else
			{
				// looking for a class
				for (uint16_t i = 0; i < numOfClasses; i++)
				{
					MsgClassInfo& cls = msgClasses[i];

					auto pos = msgsFromReplicas.find(cls.representativeReplica);
					// TODO Assert(pos is okay)
					T* representativeMsg = pos->second;

					if (ExternalFunc::equivalent(representativeMsg, msg))
					{
						cls.size = cls.size + 1;
						relevantClass = i;
						break;
					}
				}
			}


			if (relevantClass == NULL_CLASS)
			{
				// we should create a new class

				if (numOfClasses >= numOfReps)
				{
					// We probably have an internal error
					// TODO(GG): print error 
					return; // get out
				}

				relevantClass = numOfClasses;
				numOfClasses++;

				MsgClassInfo& cls = msgClasses[relevantClass];
				cls.size = 1;
				cls.representativeReplica = replicaId;

				if (bestClass == NULL_CLASS)
				{
					bestClass = relevantClass;
					sizeOfBestClass = 1;
				}
			}
			else
			{
				MsgClassInfo& cls = msgClasses[relevantClass];

				if (cls.size > sizeOfBestClass)
				{
					bestClass = relevantClass;
					sizeOfBestClass = cls.size;
				}

				if ((relevantClass == bestClass) && (sizeOfBestClass >= required))
				{
					tryToMarkComplete();
				}
			}
		}


		template <typename T, bool SelfTrust, bool SelfIsRequired, bool KeepAllMsgs, typename ExternalFunc>
		void MsgsCertificate<T, SelfTrust, SelfIsRequired, KeepAllMsgs, ExternalFunc>::addSelfMsg(T* msg)
		{
			//static_assert(SelfTrust == true, "Invalid invocation");  //TODO(GG)

			uint16_t relevantClass = NULL_CLASS;

			// looking for a class
			for (uint16_t i = 0; i < numOfClasses; i++)
			{
				MsgClassInfo& cls = msgClasses[i];

				auto pos = msgsFromReplicas.find(cls.representativeReplica);
				// TODO Assert(pos is okay)
				T* representativeMsg = pos->second;

				if (ExternalFunc::equivalent(representativeMsg, msg))
				{
					cls.size = cls.size + 1;
					relevantClass = i;
					break;
				}
			}

			MsgClassInfo classInfo;
			classInfo.representativeReplica = selfId;
			if (relevantClass == NULL_CLASS)
				classInfo.size = 1;
			else
				classInfo.size = msgClasses[relevantClass].size + 1;

			// reset msgClasses & numOfClasses
			memset(msgClasses, 0, numOfClasses * sizeof(MsgClassInfo));
			numOfClasses = 1;

			msgClasses[0] = classInfo;

			bestClass = 0;
			sizeOfBestClass = classInfo.size;

			hasTrustedSelfClass = true;

			if (classInfo.size >= required)
			{
				tryToMarkComplete();
			}
		}

		template <typename T, bool SelfTrust, bool SelfIsRequired, bool KeepAllMsgs, typename ExternalFunc>
		bool MsgsCertificate<T, SelfTrust, SelfIsRequired, KeepAllMsgs, ExternalFunc>::isComplete() const
		{
			return complete;
		}

		template <typename T, bool SelfTrust, bool SelfIsRequired, bool KeepAllMsgs, typename ExternalFunc>
		bool MsgsCertificate<T, SelfTrust, SelfIsRequired, KeepAllMsgs, ExternalFunc>::isInconsistent() const
		{
			return (numOfClasses >= maxFails + 1);
		}


		template <typename T, bool SelfTrust, bool SelfIsRequired, bool KeepAllMsgs, typename ExternalFunc>
		T* MsgsCertificate<T, SelfTrust, SelfIsRequired, KeepAllMsgs, ExternalFunc>::selfMsg() const
		{
			T* retVal = nullptr;
			auto pos = msgsFromReplicas.find(selfId);

			if (pos != msgsFromReplicas.end())
				retVal = pos->second;

			return retVal;
		}

		template <typename T, bool SelfTrust, bool SelfIsRequired, bool KeepAllMsgs, typename ExternalFunc>
		T* MsgsCertificate<T, SelfTrust, SelfIsRequired, KeepAllMsgs, ExternalFunc>::bestCorrectMsg() const
		{
			T* retVal = nullptr;
			if (hasTrustedSelfClass || (sizeOfBestClass >= maxFails + 1))
			{
				const MsgClassInfo& mci = msgClasses[bestClass];

				auto pos = msgsFromReplicas.find(mci.representativeReplica);

				// TODO: Assert pos is okay

				retVal = pos->second;
			}

			return retVal;
		}

		template <typename T, bool SelfTrust, bool SelfIsRequired, bool KeepAllMsgs, typename ExternalFunc>
		void MsgsCertificate<T, SelfTrust, SelfIsRequired, KeepAllMsgs, ExternalFunc>::tryToMarkComplete()
		{
			if (!SelfIsRequired || hasMsgFromReplica(selfId))
			{
				complete = true;
			}
		}

		template <typename T, bool SelfTrust, bool SelfIsRequired, bool KeepAllMsgs, typename ExternalFunc>
		bool MsgsCertificate<T, SelfTrust, SelfIsRequired, KeepAllMsgs, ExternalFunc>::hasMsgFromReplica(ReplicaId replicaId) const
		{
			bool retVal = (msgsFromReplicas.count(replicaId) > 0);
			return retVal;
		}

		template <typename T, bool SelfTrust, bool SelfIsRequired, bool KeepAllMsgs, typename ExternalFunc>
		std::forward_list<ReplicaId> MsgsCertificate<T, SelfTrust, SelfIsRequired, KeepAllMsgs, ExternalFunc>::includedReplicas() const // for debug
		{
			std::forward_list<ReplicaId> r;

			auto lastPos = r.end();

			for (auto i : msgsFromReplicas)
			{
				if (lastPos == r.end())
				{
					r.push_front(i->first);
					lastPos = r.begin();
				}
				else
				{
					lastPos = r.insert_after(lastPos, i->first);
				}
			}

			return r;
		}

	}
}
