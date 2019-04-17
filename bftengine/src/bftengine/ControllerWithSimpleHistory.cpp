//Concord
//
//Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
//This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in compliance with the Apache 2.0 License. 
//
//This product may include a number of subcomponents with separate copyright notices and license terms. Your use of these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE file.


#include <cmath>

#include "FullCommitProofMsg.hpp"
#include "Logger.hpp"
#include "SignedShareMsgs.hpp"
#include "ControllerWithSimpleHistory.hpp"

namespace bftEngine
{
	namespace impl
	{

		static const uint32_t defaultTimeToStartSlowPathMilli = 150; //  800; 
		static const uint32_t minTimeToStartSlowPathMilli = 20; // 100;
		static const uint32_t maxTimeToStartSlowPathMilli = 2000;
		static const float maxUpdateInTimeToStartSlowPath = 0.20F;


		ControllerWithSimpleHistory::ControllerWithSimpleHistory(
                    uint16_t C,
                    uint16_t F,
                    ReplicaId replicaId,
                    ViewNum initialView,
                    SeqNum initialSeq,
                    concordMetrics::Component &metrics) :
                  onlyOptimisticFast(C == 0),
                  c(C), f(F), numOfReplicas(3 * F + 2 * C + 1), myId(replicaId),
                  recentActivity(1, nullptr),
                  currentFirstPath{ControllerWithSimpleHistory_debugInitialFirstPath},
                  currentView{initialView},
                  isPrimary{((currentView % numOfReplicas) == myId)},
                  currentTimeToStartSlowPathMilli{defaultTimeToStartSlowPathMilli},
                  metrics_first_path_{metrics.RegisterStatus(
                      "firstCommitPath",
                      CommitPathToStr(currentFirstPath))}
                {
			if (isPrimary) {
                          onBecomePrimary(initialView, initialSeq);
                        }

		}

		void ControllerWithSimpleHistory::onBecomePrimary(ViewNum v, SeqNum s)
		{
			Assert(isPrimary);

			SeqNum nextFirstRelevantSeqNum;
			if (s % EvaluationPeriod == 0) nextFirstRelevantSeqNum = s + 1;
			else nextFirstRelevantSeqNum = ((s / EvaluationPeriod) * EvaluationPeriod) + EvaluationPeriod + 1;

			recentActivity.resetAll(nextFirstRelevantSeqNum);

			currentTimeToStartSlowPathMilli = defaultTimeToStartSlowPathMilli;

			LOG_INFO_F(GL, "currentTimeToStartSlowPathMilli = %d", (int)currentTimeToStartSlowPathMilli);
		}


		CommitPath ControllerWithSimpleHistory::getCurrentFirstPath()
		{
			return currentFirstPath;
		}


		uint32_t ControllerWithSimpleHistory::timeToStartSlowPathMilli()
		{
			return currentTimeToStartSlowPathMilli;
		}


		uint32_t ControllerWithSimpleHistory::slowPathsTimerMilli()
		{
			const uint32_t minUsefulTimerRes = 10; // TODO(GG): ??

			uint32_t retVal = (currentTimeToStartSlowPathMilli / 2);
			if (retVal < minUsefulTimerRes) retVal = minUsefulTimerRes;

			return retVal;
		}



		void ControllerWithSimpleHistory::onNewView(ViewNum v, SeqNum s)
		{
			Assert(v >= currentView);
			currentView = v;
			isPrimary = ((currentView % numOfReplicas) == myId);

			if (isPrimary)
				onBecomePrimary(v, s);
		}

		void ControllerWithSimpleHistory::onNewSeqNumberExecution(SeqNum n)
		{
			if (!isPrimary || !recentActivity.insideActiveWindow(n)) return;

			SeqNoInfo& s = recentActivity.get(n);

			Time now = getMonotonicTime();

			uint64_t duration = 0;
			if (now > s.prePrepareTime)
				duration = subtract(now, s.prePrepareTime);

			const uint64_t MAX_DURATION_MICRO = 2 * 1000 * 1000; // 2 sec
			const uint64_t MIN_DURATION_MICRO = 100; // 100 micro

			if (duration > MAX_DURATION_MICRO) duration = MAX_DURATION_MICRO;
			else if (duration < MIN_DURATION_MICRO) duration = MIN_DURATION_MICRO;

			s.durationMicro = (uint16_t)duration;

			if (n % 1000 == 0) avgAndStdOfExecTime.reset(); // reset every 1000 rounds . TODO(GG): in config ?

			avgAndStdOfExecTime.add((double)duration);

			if (n == recentActivity.currentActiveWindow().second) onEndOfEvaluationPeriod();
		}

		void ControllerWithSimpleHistory::onEndOfEvaluationPeriod()
		{
			const SeqNum minSeq = recentActivity.currentActiveWindow().first;
			const SeqNum maxSeq = recentActivity.currentActiveWindow().second;

			const CommitPath lastFirstPathVal = currentFirstPath;

			bool downgraded = false;

			if (currentFirstPath == CommitPath::SLOW)
			{
				size_t cyclesWithFullCooperation = 0;
				size_t cyclesWithPartialCooperation = 0;

				for (SeqNum i = minSeq; i <= maxSeq; i++)
				{
					SeqNoInfo& s = recentActivity.get(i);
					if (s.replicas.size() == (numOfReplicas - 1)) cyclesWithFullCooperation++;
					else if (s.replicas.size() >= (3 * f + c)) cyclesWithPartialCooperation++;
				}

				if (ControllerWithSimpleHistory_debugUpgradeEnabled)
				{

					const float factorForFastOptimisticPath = ControllerWithSimpleHistory_debugUpgradeFactorForFastOptimisticPath; // 0.95F;
					const float factorForFastPath = ControllerWithSimpleHistory_debugUpgradeFactorForFastPath; //  0.85F;

					if (cyclesWithFullCooperation >= (factorForFastOptimisticPath * EvaluationPeriod)) {
						currentFirstPath = CommitPath::OPTIMISTIC_FAST;
                                                metrics_first_path_.Get().Set(CommitPathToStr(currentFirstPath));
                                        }
					else if (!onlyOptimisticFast && (cyclesWithFullCooperation + cyclesWithPartialCooperation >= (factorForFastPath * EvaluationPeriod))) {
						currentFirstPath = CommitPath::FAST_WITH_THRESHOLD;
                                                metrics_first_path_.Get().Set(CommitPathToStr(currentFirstPath));
                                        }
				}
			}
			else
			{
				Assert(currentFirstPath == CommitPath::OPTIMISTIC_FAST || currentFirstPath == CommitPath::FAST_WITH_THRESHOLD);

				size_t successfulFastPaths = 0;
				for (SeqNum i = minSeq; i <= maxSeq; i++)
				{
					SeqNoInfo& s = recentActivity.get(i);
					if (!s.switchToSlowPath) successfulFastPaths++;
				}

				const float factor = ControllerWithSimpleHistory_debugDowngradeFactor; // 0.85F; //  0.0F;

				if (ControllerWithSimpleHistory_debugDowngradeEnabled && (successfulFastPaths < (factor * EvaluationPeriod)))
				{
					downgraded = true;

					// downgrade
					if (!onlyOptimisticFast && (currentFirstPath == CommitPath::OPTIMISTIC_FAST)) {
						currentFirstPath = CommitPath::FAST_WITH_THRESHOLD;
                                        } else {
						currentFirstPath = CommitPath::SLOW;
                                        }

                                        metrics_first_path_.Get().Set(CommitPathToStr(currentFirstPath));
				}
			}

			recentActivity.resetAll(maxSeq + 1);

			if (lastFirstPathVal != currentFirstPath)
				LOG_INFO_F(GL, "currentFirstPath = %d", (int)currentFirstPath);

			const double execAvg = avgAndStdOfExecTime.avg();
			const double execVar = avgAndStdOfExecTime.var();
			const double execStd = ((execVar) > 0 ? sqrt(execVar) : 0);

			LOG_INFO_F(GL, "Execution time of recent rounds (micro) - avg=%f std=%f", execAvg, execStd);

			if (!downgraded)
			{
				// compute and update currentTimeToStartSlowPathMilli
				const uint32_t relMin = (uint32_t)((1 - maxUpdateInTimeToStartSlowPath)*currentTimeToStartSlowPathMilli);
				const uint32_t relMax = (uint32_t)((1 + maxUpdateInTimeToStartSlowPath)*currentTimeToStartSlowPathMilli);

				uint32_t newSlowPathTimeMilli = (uint32_t)((execAvg + 2 * execStd) / 1000);

				if (newSlowPathTimeMilli < relMin) newSlowPathTimeMilli = relMin;
				else if (newSlowPathTimeMilli > relMax) newSlowPathTimeMilli = relMax;

				if (newSlowPathTimeMilli < minTimeToStartSlowPathMilli) newSlowPathTimeMilli = minTimeToStartSlowPathMilli;
				else if (newSlowPathTimeMilli > maxTimeToStartSlowPathMilli) newSlowPathTimeMilli = maxTimeToStartSlowPathMilli;

				currentTimeToStartSlowPathMilli = newSlowPathTimeMilli;

				LOG_INFO_F(GL, "currentTimeToStartSlowPathMilli = %d", (int)currentTimeToStartSlowPathMilli);
			}
		}

		void ControllerWithSimpleHistory::onSendingPrePrepare(SeqNum n, CommitPath commitPath)
		{
			if (!isPrimary || !recentActivity.insideActiveWindow(n)) return;

			SeqNoInfo& s = recentActivity.get(n);

			if (s.prePrepareTime == 0) // if this is the time we send the preprepare 
				s.prePrepareTime = getMonotonicTime();
		}

		void ControllerWithSimpleHistory::onStartingSlowCommit(SeqNum n)
		{
			if (!isPrimary || !recentActivity.insideActiveWindow(n)) return;
			if (currentFirstPath == CommitPath::SLOW) return;

			SeqNoInfo& s = recentActivity.get(n);
			s.switchToSlowPath = true;
		}


		void ControllerWithSimpleHistory::onMessage(const PreparePartialMsg* m)
		{
			if (!isPrimary) return;
			if (currentFirstPath != CommitPath::SLOW) return;

			const SeqNum n = m->seqNumber();
			const ReplicaId id = m->senderId();

			if (!recentActivity.insideActiveWindow(n)) return;
			SeqNoInfo& s = recentActivity.get(n);
			if (s.switchToSlowPath) return;

			if (s.replicas.count(id) > 0) return;
			s.replicas.insert(id);
		}

                std::string CommitPathToStr(CommitPath path) {
                  switch(path) {
                    case CommitPath::NA:
                      return "NA";
                    case CommitPath::OPTIMISTIC_FAST:
                      return "OPTIMISTIC_FAST";
                    case CommitPath::FAST_WITH_THRESHOLD:
                      return "FAST_WITH_THRESHOLD";
                    case CommitPath::SLOW:
                      return "SLOW";
                  }
                }
	}
}
