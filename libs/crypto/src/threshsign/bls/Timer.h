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

#include <chrono>
#include <string>

#include "XAssert.h"

using std::chrono::microseconds;

/**
 * To be used as:
 * 	{
 * 		ScopedTimer t(std::cout, "My code took ");
 * 		mycode();
 * 	}
 */
class ScopedTimer {
 private:
  typedef std::chrono::high_resolution_clock theclock;
  std::chrono::time_point<theclock> beginning;
  std::ostream& out;
  std::string prefix;
  const char* suffix;

 public:
  ScopedTimer(std::ostream& out, const std::string& prefix = "", const char* suffix = "\n")
      : beginning(theclock::now()), out(out), prefix(prefix), suffix(suffix) {}

  ~ScopedTimer() {
    microseconds mus = std::chrono::duration_cast<microseconds>(theclock::now() - beginning);
    out << prefix << mus.count() << " microseconds" << suffix << std::flush;
  }
};

/**
 * To be used as:
 *
 * 	ManualTimer t;
 * 	mycode();
 * 	microseconds duration = t.stop();
 *
 * Or as:
 *
 *  ManualTimer t;
 *  mycode1();
 *  microseconds duration1 = t.restart();
 *  mycode2();
 *  microseconds duration 2= t.restart();
 *  // and so on...
 */
class ManualTimer {
 private:
  typedef std::chrono::high_resolution_clock theclock;
  std::chrono::time_point<theclock> beginning;

 public:
  ManualTimer() : beginning(theclock::now()) {}

  microseconds restart() {
    microseconds mus = std::chrono::duration_cast<microseconds>(theclock::now() - beginning);
    beginning = theclock::now();
    return mus;
  }

  microseconds stop() const { return std::chrono::duration_cast<microseconds>(theclock::now() - beginning); }
};

/**
 * To be used for timing multiple pieces of code in a loop:
 *
 * 	AveragingTimer c1, c2;
 * 	for(int i = 0; i < numLaps; i++) {
 * 		c1.startLap();
 *		mycode1();
 * 		c1.endLap();
 *
 * 		c2.startLap();
 * 		mycode2();
 * 		c2.endLap();
 * 	}
 *
 * 	std::cout << "mycode1() average lap time: " << c1.averageLapTime() << std::endl;
 * 	std::cout << "mycode2() average lap time: " << c2.averageLapTime() << std::endl;
 */
class AveragingTimer {
 private:
  typedef std::chrono::high_resolution_clock theclock;
  std::chrono::time_point<theclock> beginning;

  microseconds total;
  unsigned long iters;
  bool started;
  std::string name;

 private:
  friend std::ostream& operator<<(std::ostream& out, const AveragingTimer& t);

 public:
  AveragingTimer(const std::string& name = "some timer") : total(0), iters(0), started(false), name(name) {}

 public:
  void startLap() {
    assertFalse(started);
    started = true;
    beginning = theclock::now();
  }

  const std::string& getName() const { return name; }

  microseconds endLap() {
    microseconds duration = std::chrono::duration_cast<microseconds>(theclock::now() - beginning);
    total += duration;
    iters++;
    assertTrue(started);  // Do this after timing, to not slow down anything.
    started = false;
    return duration;
  }

  unsigned long numIterations() const { return iters; }

  microseconds::rep totalLapTime() const { return total.count(); }

  microseconds::rep averageLapTime() const { return total.count() / static_cast<microseconds::rep>(iters); }
};
