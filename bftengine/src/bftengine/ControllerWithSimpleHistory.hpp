//Concord
//
//Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
//This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in compliance with the Apache 2.0 License. 
//
//This product may include a number of subcomponents with separate copyright notices and license terms. Your use of these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE file.

#pragma once

#include <set>

#include "ControllerBase.hpp"
#include "SysConsts.hpp"
#include "SequenceWithActiveWindow.hpp"
#include "RollingAvgAndVar.hpp"
#include "TimeUtils.hpp"

namespace bftEngine
{
	namespace impl
	{

		class ControllerWithSimpleHistory : public ControllerBase
		{
		public:

			static const size_t EvaluationPeriod = 64;

			ControllerWithSimpleHistory(
                            uint16_t C,
                            uint16_t F,
                            ReplicaId replicaId,
                            ViewNum initialView,
                            SeqNum initialSeq);

			// getter methods

			virtual CommitPath getCurrentFirstPath() override;
			virtual uint32_t timeToStartSlowPathMilli() override;
			virtual uint32_t slowPathsTimerMilli() override;

			// events 

			virtual void onNewView(ViewNum v, SeqNum s) override;
			virtual bool onNewSeqNumberExecution(SeqNum n) override;

			virtual void onSendingPrePrepare(SeqNum n, CommitPath commitPath) override;
			virtual void onStartingSlowCommit(SeqNum n) override;
			virtual void onMessage(const PreparePartialMsg* m) override;

			class SeqNoInfo
			{
			public:
				SeqNoInfo() : switchToSlowPath(false), prePrepareTime(0), durationMicro(0) {}

				void resetAndFree() { switchToSlowPath = false;  prePrepareTime = 0;  durationMicro = 0;  replicas.clear(); }
			private:
				bool switchToSlowPath;

				Time prePrepareTime;
				uint16_t durationMicro;

				// used when currentFirstPath == SLOW
				std::set<ReplicaId> replicas; // replicas that have responded for this sequence number

				friend class ControllerWithSimpleHistory;

			public:
				// methods for SequenceWithActiveWindow
				static void init(SeqNoInfo& i, void* d)
				{
				}

				static void free(SeqNoInfo& i)
				{
					i.resetAndFree();
				}

				static void reset(SeqNoInfo& i)
				{
					i.resetAndFree();
				}
			};

		protected:
			const bool onlyOptimisticFast;
			const size_t c;
			const size_t f;
			const size_t numOfReplicas;
			const ReplicaId myId;

			SequenceWithActiveWindow<EvaluationPeriod, 1, SeqNum, SeqNoInfo, SeqNoInfo> recentActivity;

			CommitPath currentFirstPath;
			ViewNum currentView;
			bool isPrimary;

			RollingAvgAndVar avgAndStdOfExecTime;

			uint32_t currentTimeToStartSlowPathMilli;

			void onBecomePrimary(ViewNum v, SeqNum s);
			bool onEndOfEvaluationPeriod();
		};


	}
}
