// Copyright (c) 2018 VMware. All rights reserved.

#include "status.hpp"

#include <ostream>

namespace concordUtils {

std::ostream& operator<<(std::ostream& s, Status const& status) { return status.operator<<(s); }

}  // namespace concordUtils
