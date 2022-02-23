#pragma once

#include <cinttypes>
#include <map>
#include <optional>
#include <set>
#include <variant>
#include <vector>

using nat = uint64_t;
using ViewNum = nat;
using SequenceID = nat;
using HostId = nat;
using std::map;
using std::set;

using std::variant;

template <class T>
class Option : public std::optional<T> {
 public:
  bool Some() { return this->has_value(); }
};
