//Concord
//
//Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
//This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in compliance with the Apache 2.0 License. 
//
//This product may include a number of subcomponents with separate copyright notices and license terms. Your use of these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE file.

#include "SimpleThreadPool.hpp"
#include "Threading.h"

namespace bftEngine
{
	namespace impl
	{

		struct InternalDataOfSimpleThreadPool
		{
			SimpleThreadPool::Controller* pController;

			Mutex jobQueue_lock;
			CondVar jobQueue_cond;
			CondVar completion_cond;

			bool stopped;
			queue<SimpleThreadPool::Job*> jobQueue;
			Thread* threadsArray;

			int runningJobs;
		};

#if defined(_WIN32)
		static DWORD WINAPI threadExecute(LPVOID param)
#else
		static void* threadExecute(void *param)
#endif
		{
			SimpleThreadPool* obj = (SimpleThreadPool*)param;

			if (obj->p->pController) obj->p->pController->onThreadBegin();

			SimpleThreadPool::Job *j = nullptr;
			while (obj->load(j))
			{
				if (j) {
					j->execute();
					j->release();
					j = nullptr;
					mutexLock(&obj->p->jobQueue_lock);
					obj->p->runningJobs--;
					mutexUnlock(&obj->p->jobQueue_lock);
				}
			}


			if (obj->p->pController) obj->p->pController->onThreadEnd();

			return 0;
		}

		SimpleThreadPool::SimpleThreadPool() : SimpleThreadPool(1)
		{}

		SimpleThreadPool::SimpleThreadPool(unsigned char numOfThreads) : numberOfThreads(numOfThreads), p(new InternalDataOfSimpleThreadPool)
		{
			init(&p->jobQueue_lock);
			init(&p->jobQueue_cond);
			init(&p->completion_cond);

			p->stopped = true;
			p->threadsArray = nullptr;
			p->runningJobs = 0;
			p->pController = nullptr;
		}



		SimpleThreadPool::~SimpleThreadPool()
		{
			// TODO(GG): if still running, then safely stop the threads

			if (p->pController) delete p->pController;

			destroy(&p->jobQueue_lock);

			//TODO(GG): free more resources ?? 

			delete p;
		}

		void SimpleThreadPool::start(Controller* c)
		{
			// TODO: ALIN: if(!stopped) throw std::runtime_error("cannot restart");
			// TODO(GG): should be thread safe

			p->pController = c;
			p->stopped = false;
			p->threadsArray = new Thread[numberOfThreads];
			for (int i = 0; i < numberOfThreads; i++)
			{
				createThread(&p->threadsArray[i], &threadExecute, (void*)this);
			}
		}

		void SimpleThreadPool::stop(bool executeAllJobs)
		{
			// TODO: ALIN: if(stopped) throw std::runtime_error("cannot stop twice");
			// TODO(GG): should be thread safe

			mutexLock(&p->jobQueue_lock);
			p->stopped = true;
			broadcastSignal(&p->jobQueue_cond);
			mutexUnlock(&p->jobQueue_lock);

			for (int i = 0; i < numberOfThreads; i++)
				threadJoin(p->threadsArray[i]);

			delete[] p->threadsArray;

			while (!p->jobQueue.empty())
			{
				Job* j = p->jobQueue.front();
				p->jobQueue.pop();
				if (j) {
					if (executeAllJobs) j->execute();
					j->release();
				}
			}

			broadcastSignal(&p->completion_cond);

			// TODO(GG): TBD - free resources (e.g. condition vars) ??
		}

		void SimpleThreadPool::add(Job* j)
		{
			if (p->stopped) return; // error

			mutexLock(&p->jobQueue_lock);
			p->jobQueue.push(j);
			singleSignal(&p->jobQueue_cond);
			mutexUnlock(&p->jobQueue_lock);
		}

		void SimpleThreadPool::waitForCompletion()
		{
			mutexLock(&p->jobQueue_lock);
			while (!p->jobQueue.empty() || p->runningJobs > 0)
				waitCondVar(&p->completion_cond, &p->jobQueue_lock);
			mutexUnlock(&p->jobQueue_lock);
		}

		bool SimpleThreadPool::load(Job*& outJob)
		{
			mutexLock(&p->jobQueue_lock);
			while (p->jobQueue.empty() && !p->stopped)
			{
				broadcastSignal(&p->completion_cond);
				waitCondVar(&p->jobQueue_cond, &p->jobQueue_lock);
			}

			if (!p->stopped)
			{
				outJob = p->jobQueue.front();
				p->jobQueue.pop();
				p->runningJobs++;
			}
			mutexUnlock(&p->jobQueue_lock);
			return !p->stopped;
		}


	}
}
