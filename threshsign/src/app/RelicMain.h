#pragma once

#include <string>
#include <vector>

namespace BLS {
namespace Relic {
class Library;
}
}  // namespace BLS

int RelicAppMain(const BLS::Relic::Library& lib, const std::vector<std::string>& args);
