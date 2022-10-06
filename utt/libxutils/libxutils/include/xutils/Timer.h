/*
 * Timer.h
 *
 * Author: Alin Tomescu <alinush@mit.edu>
 *
 * TODO: Add wall-clock timer timers versus CPU time timers
 */
#pragma once

#include <chrono>
#include <cmath>
#include <limits>
#include <string>

#include <xassert/XAssert.h>
#include <xutils/Utils.h>

using std::chrono::microseconds;

namespace libutt {

/**
 * To be used as:
 * 	{
 * 		ScopedTimer t(std::cout, "My code took ");
 * 		mycode();
 * 	}
 */
template <class T = std::chrono::microseconds>
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
    T mus = std::chrono::duration_cast<T>(theclock::now() - beginning);
    out << prefix << Utils::withCommas(mus.count()) << " microseconds" << suffix << std::flush;
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

class CumulativeTimer {
 protected:
  typedef std::chrono::high_resolution_clock theclock;
  microseconds total;
  std::chrono::time_point<theclock> beginning;
  bool started;

 public:
  CumulativeTimer() : total(0), started(false) {}

 public:
  microseconds::rep getTotal() const { return total.count(); }

  void start() {
    assertFalse(started);
    started = true;
    beginning = theclock::now();
  }

  /**
   * Returns the duration of the current lap.
   */
  microseconds::rep end() {
    microseconds duration = std::chrono::duration_cast<microseconds>(theclock::now() - beginning);

    total += duration;

    assertTrue(started);  // Do this after timing, to not slow down anything.
    started = false;
    return duration.count();
  }
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

  microseconds total, min, max;
  unsigned long iters;
  double m2;  // used to calculate variance
  bool started;
  std::string name;

 private:
  friend std::ostream& operator<<(std::ostream& out, const AveragingTimer& t);

 public:
  AveragingTimer(const std::string& name = "some timer")
      : total(0),
        min(std::chrono::microseconds::max().count()),
        max(std::chrono::microseconds::min().count()),
        iters(0),
        m2(0.0),
        started(false),
        name(name) {}

 public:
  void startLap() {
    assertFalse(started);
    started = true;
    beginning = theclock::now();
  }

  const std::string& getName() const { return name; }

  microseconds::rep endLap() {
    microseconds duration = std::chrono::duration_cast<microseconds>(theclock::now() - beginning);

    double delta = static_cast<double>(duration.count()) - mean();
    iters++;  // WARNING: Moving this line up will affect the mean() calculation above. Don't do it!
    total += duration;
    double delta2 = static_cast<double>(duration.count()) - mean();
    m2 += delta * delta2;

    assertTrue(started);  // Do this after timing, to not slow down anything.
    started = false;

    // update min & max
    if (min > duration) min = duration;
    if (max < duration) max = duration;

    return duration.count();
  }

  unsigned long numIterations() const { return iters; }

  double variance() const { return m2 / static_cast<double>(iters); }

  double stddev() const {
    if (iters < 2) {
      return std::numeric_limits<double>::quiet_NaN();
    } else {
      auto var = variance();
      if (var < 0) throw std::runtime_error("Variance is supposed to always be positive");
      return sqrt(variance());
    }
  }

  double mean() const {
    if (iters == 0)
      return 0;
    else
      return static_cast<double>(total.count()) / static_cast<double>(iters);
  }

  microseconds::rep totalLapTime() const { return total.count(); }

  microseconds::rep averageLapTime() const {
    if (iters == 0) throw std::runtime_error("Cannot compute average lap time when no laps were started");
    return total.count() / static_cast<microseconds::rep>(iters);
  }
};

}  // namespace libutt
