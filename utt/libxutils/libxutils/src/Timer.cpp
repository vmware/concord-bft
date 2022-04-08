/*
 * Timer.cpp
 *
 *  Created on: Aug 25, 2017
 *      Author: alinush
 */

#include <xutils/Timer.h>
#include <xutils/Utils.h>

#include <iostream>

std::ostream& operator<<(std::ostream& out, const AveragingTimer& t) {
    if(t.numIterations() > 0)
        out << t.name << ": "
            << Utils::humanizeMicroseconds(t.averageLapTime()) << " per lap"
            << ", -/+ "
            << t.stddev() << " mus stddev"
            << ", " << Utils::humanizeMicroseconds(t.min.count()) << " min"
            << ", " << Utils::humanizeMicroseconds(t.max.count()) << " max"
            << " (" << t.numIterations() << " laps)";
    else
        out << t.name << ": did not run any laps yet.";
    return out;
}
