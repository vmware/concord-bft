//Concord
//
//Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
//This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in compliance with the Apache 2.0 License. 
//
//This product may include a number of subcomponents with separate copyright notices and license terms. Your use of these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE file.


#include <chrono>
#include <forward_list>
#include "TimeUtils.hpp"
#include "assertUtils.hpp"
 
///////////////////////////////////////////////////////////////////////////////
// Time
///////////////////////////////////////////////////////////////////////////////

namespace bftEngine
{
	namespace impl
	{

		Time getMonotonicTime()
		{
			std::chrono::system_clock::time_point curTimePoint = std::chrono::system_clock::now();

			auto timeSinceEpoch = curTimePoint.time_since_epoch();

			uint64_t micro = std::chrono::duration_cast<std::chrono::microseconds>(timeSinceEpoch).count();

			return micro;
		}


		///////////////////////////////////////////////////////////////////////////////
		// SimpleOperationsScheduler
		///////////////////////////////////////////////////////////////////////////////

		SimpleOperationsScheduler::SimpleOperationsScheduler() :
			_items()
		{
		}

		SimpleOperationsScheduler::~SimpleOperationsScheduler()
		{
		}

		void SimpleOperationsScheduler::clear()
		{
			_items.clear();
		}

		bool SimpleOperationsScheduler::add(uint64_t id, Time time, void(*opFunc)(uint64_t, Time, void*), void* param)
		{
			if (!_items.empty())
			{
				const size_t last = _items.size() - 1;

				for (size_t i = 0; i <= last; i++)
					if (_items[i].id == id) return false;
			}

			Item x;
			x.id = id;
			x.time = time;
			x.opFunc = opFunc;
			x.param = param;

			_items.push_back(x);

			return true;
		}

		bool SimpleOperationsScheduler::remove(uint64_t id)
		{
			if (_items.empty()) return false;

			const size_t last = _items.size() - 1;

			for (size_t i = 0; i <= last; i++)
			{
				if (_items[i].id == id)
				{
					_items[i] = _items[last];
					_items.pop_back();
					return true;
				}
			}

			return false;
		}



		void SimpleOperationsScheduler::evaluate()
		{
			if (_items.empty()) return;

			const Time currTime = getMonotonicTime();
			size_t last = _items.size() - 1;
			size_t i = 0;
			std::forward_list<Item> readyOperations;

			while (i <= last)
			{
				const Item& x = _items[i];
				if (currTime >= x.time)
				{
					readyOperations.push_front(x);

					_items[i] = _items[last];
					_items.pop_back();

					if (last == 0) break;

					last--;
				}
				else
				{
					i++;
				}
			}

			for (Item x : readyOperations)
			{
				x.opFunc(x.id, currTime, x.param);
			}
		}


		///////////////////////////////////////////////////////////////////////////////
		// Timer
		///////////////////////////////////////////////////////////////////////////////

		Timer::Timer(SimpleOperationsScheduler& operationsScheduler, uint16_t timePeriodMilli, void(*opFunc)(Time, void*), void* param)
			: _scheduler(operationsScheduler), _active(false), _periodMilli(timePeriodMilli), _userFunc(opFunc), _param(param)
		{
			// TODO(GG): asserts are needed here ....
		}

		Timer::~Timer()
		{
			if (_active)
			{
				bool r = _scheduler.remove((uint64_t)this);
				Assert(r);
			}
		}

		bool Timer::start()
		{
			if (_active) return false;

			Time t = getMonotonicTime() + _periodMilli * 1000;

			bool r = _scheduler.add((uint64_t)this, t, onTimeExpired, _param);
			Assert(r);

			_active = true;
			return true;
		}

		bool Timer::stop()
		{
			if (!_active) return false;

			bool r = _scheduler.remove((uint64_t)this);
			Assert(r);

			_active = false;
			return true;
		}

		void Timer::changePeriodMilli(uint16_t timePeriodMilli)
		{
			// TODO(GG): Assert(timePeriodMilli > 0)

			_periodMilli = timePeriodMilli;
		}

		void Timer::onTimeExpired(uint64_t id, Time time, void* p)
		{
			Timer* timer = (Timer*)id;
			timer->_active = false;

			timer->_userFunc(time, p); // call to user method (notice that it may start the timer again)
		}

	}
}
