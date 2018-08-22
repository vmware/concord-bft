//Concord
//
//Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
//This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in compliance with the Apache 2.0 License. 
//
//This product may include a number of subcomponents with separate copyright notices and license terms. Your use of these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE file.


#pragma once

#include <stdint.h>
#include <vector>

namespace bftEngine
{
	namespace impl
	{

		///////////////////////////////////////////////////////////////////////////////
		// Time
		///////////////////////////////////////////////////////////////////////////////

		typedef  uint64_t Time;
		typedef  int64_t TimeDeltaMirco;

		const Time MaxTime = UINT64_MAX;
		const Time MinTime = 0;

		Time getMonotonicTime();

		inline TimeDeltaMirco subtract(Time t1, Time t2)  // TODO(GG): replace with absDifference (not sure that this method is really needed)
		{
			return (t1 - t2);
		}

		inline Time addMilliseconds(Time base, uint16_t milliseconds)
		{
			return (base + (milliseconds * 1000));
		}

		inline TimeDeltaMirco absDifference(Time t1, Time t2)
		{
			if (t2 < t1)
				return (t1 - t2);
			else
				return (t2 - t1);
		}

		///////////////////////////////////////////////////////////////////////////////
		// SimpleOperationsScheduler
		///////////////////////////////////////////////////////////////////////////////

		class SimpleOperationsScheduler
		{
			// designed to handle a small number of operations
			// TODO(GG): consider to optimize (this is a simplified implementation)
			// TODO(GG): consider creating a generic scheduler
		public:
			SimpleOperationsScheduler();
			~SimpleOperationsScheduler();

			void clear();
			bool add(uint64_t id, Time time, void(*opFunc)(uint64_t, Time, void*), void* param);
			bool remove(uint64_t id);

			void evaluate();

		protected:

			struct Item
			{
				uint64_t id;
				Time time;
				void(*opFunc)(uint64_t, Time, void*);
				void* param;
			};
			std::vector<Item> _items;
		};

		///////////////////////////////////////////////////////////////////////////////
		// Timer
		///////////////////////////////////////////////////////////////////////////////

		class Timer
		{
		public:
			Timer(SimpleOperationsScheduler& operationsScheduler, uint16_t timePeriodMilli, void(*opFunc)(Time, void*), void* param = nullptr);
			~Timer();

			bool start();
			bool stop();

			void changePeriodMilli(uint16_t timePeriodMilli); // will be used after the next start

			bool isActive() const { return _active; }

		protected:

			static void onTimeExpired(uint64_t, Time, void*);

			SimpleOperationsScheduler& _scheduler;
			bool _active;
			uint16_t _periodMilli;
			void(*_userFunc)(Time, void*);
			void* _param;
		};

	}
}
