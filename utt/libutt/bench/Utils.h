#pragma once

#include <utt/Dkg.h>

#include <string>
#include <stdexcept>

#include <boost/algorithm/string/predicate.hpp> // for boost::starts_with(str, pattern)

bool needsKatePublicParams(const std::vector<std::string>& types) {
    bool needsKpp = false;

    for(auto& t : types) {
        if(boost::starts_with(t, "amt") || boost::starts_with(t, "kate") || boost::starts_with(t, "fk")) {
            needsKpp = true;
            break;
        }
    }

    return needsKpp;
}

std::string sumAvgTimeOrNan(const std::vector<AveragingTimer>& timers, bool humanize) {
    microseconds::rep sum = 0;

    for(const auto& t : timers) {
        if(t.numIterations() > 0) {
            sum += t.averageLapTime();
        } else {
            return "nan";
        }
    }

    if(humanize)
        return Utils::humanizeMicroseconds(sum, 2);
    else
        return std::to_string(sum);
}

std::string avgTimeOrNan(const AveragingTimer& t, bool humanize) {
    auto v = {t};
    return sumAvgTimeOrNan(v, humanize);
}

std::string stddevOrNan(const AveragingTimer& t) {
    return t.numIterations() > 0 ? std::to_string(t.stddev()) : "nan";
}
