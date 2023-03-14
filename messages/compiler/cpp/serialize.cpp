#include <algorithm>
#include <array>
#include <cassert>
#include <cstdint>
#include <map>
#include <optional>
#include <sstream>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

namespace cmf {

inline void cmfAssert(bool expr) {
  if (expr != true) {
    std::terminate();
  }
}

class DeserializeError : public std::runtime_error {
 public:
  DeserializeError(const std::string& error) : std::runtime_error(("DeserializeError: " + error).c_str()) {}
};

class NoDataLeftError : public DeserializeError {
 public:
  NoDataLeftError() : DeserializeError("Data left in buffer is less than what is needed for deserialization") {}
};

class BadDataError : public DeserializeError {
 public:
  BadDataError(const std::string& expected, const std::string& got) : DeserializeError(str(expected, got)) {}

 private:
  static std::string str(const std::string& expected, const std::string& actual) {
    std::ostringstream oss;
    oss << "Expected " << expected << ", got" << actual;
    return oss.str();
  }
};

/******************************************************************************
 * Integers
 *
 * All integers are encoded in big-endian
 ******************************************************************************/
template <typename T, typename std::enable_if<std::is_integral<T>::value>::type* = nullptr>
void serialize(std::vector<uint8_t>& output, const T& t) {
  if constexpr (std::is_same_v<T, bool>) {
    output.push_back(t ? 1 : 0);
  } else {
    for (auto i = sizeof(T); i > 0; i--) {
      output.push_back(255 & (t >> ((i - 1) * 8)));
    }
  }
}

template <typename T, typename std::enable_if<std::is_integral<T>::value>::type* = nullptr>
void serialize(std::string& output, const T& t) {
  if constexpr (std::is_same_v<T, bool>) {
    output.push_back(t ? 1 : 0);
  } else {
    for (auto i = sizeof(T); i > 0; i--) {
      output.push_back(255 & (t >> ((i - 1) * 8)));
    }
  }
}

template <typename T, typename std::enable_if<std::is_integral<T>::value>::type* = nullptr>
void deserialize(const uint8_t*& start, const uint8_t* end, T& t) {
  if constexpr (std::is_same_v<T, bool>) {
    if (start + 1 > end) {
      throw NoDataLeftError();
    }
    if (*start == 0) {
      t = false;
    } else if (*start == 1) {
      t = true;
    } else {
      throw BadDataError("0 or 1", std::to_string(*start));
    }
    start += 1;
  } else {
    if (start + sizeof(T) > end) {
      throw NoDataLeftError();
    }
    t = 0;
    for (auto i = 0u; i < sizeof(T); i++) {
      t |= (static_cast<std::make_unsigned_t<T>>(*(start + sizeof(T) - 1 - i)) << (i * 8));
    }
    start += sizeof(T);
  }
}

/******************************************************************************
 * Enums
 *
 * Enums are type wrappers around uint8_t
 ******************************************************************************/
template <typename T, typename std::enable_if<std::is_enum<T>::value>::type* = nullptr>
void serialize(std::vector<uint8_t>& output, const T& t) {
  serialize(output, static_cast<uint8_t>(t));
}

template <typename T, typename std::enable_if<std::is_enum<T>::value>::type* = nullptr>
void serialize(std::string& output, const T& t) {
  serialize(output, static_cast<uint8_t>(t));
}

template <typename T, typename std::enable_if<std::is_enum<T>::value>::type* = nullptr>
void deserialize(const uint8_t*& start, const uint8_t* end, T& t) {
  uint8_t val;
  deserialize(start, end, val);
  t = static_cast<T>(val);
  if (val >= enumSize(t)) {
    throw BadDataError(std::string("Value < ") + std::to_string(enumSize(t)), std::to_string(val));
  }
}

/******************************************************************************
 * Strings
 *
 * Strings are preceded by a uint32_t length
 ******************************************************************************/
[[maybe_unused]] static inline void serialize(std::vector<uint8_t>& output, const std::string& s) {
  cmfAssert(s.size() <= 0xFFFFFFFF);
  uint32_t length = s.size() & 0xFFFFFFFF;
  serialize(output, length);
  std::copy(s.begin(), s.end(), std::back_inserter(output));
}

[[maybe_unused]] static inline void serialize(std::string& output, const std::string& s) {
  cmfAssert(s.size() <= 0xFFFFFFFF);
  uint32_t length = s.size() & 0xFFFFFFFF;
  serialize(output, length);
  std::copy(s.begin(), s.end(), std::back_inserter(output));
}

[[maybe_unused]] static inline void deserialize(const uint8_t*& start, const uint8_t* end, std::string& s) {
  uint32_t length;
  deserialize(start, end, length);
  if (start + length > end) {
    throw NoDataLeftError();
  }
  std::copy_n(start, length, std::back_inserter(s));
  start += length;
}

/******************************************************************************
 Forward declarations needed by recursive types
 ******************************************************************************/
// Lists
template <typename T>
void serialize(std::vector<uint8_t>& output, const std::vector<T>& v);
template <typename T>
void serialize(std::string& output, const std::vector<T>& v);
template <typename T>
void deserialize(const uint8_t*& start, const uint8_t* end, std::vector<T>& v);

// Fixed Lists
template <typename T, std::size_t N>
void serialize(std::vector<uint8_t>& output, const std::array<T, N>& v);
template <typename T, std::size_t N>
void serialize(std::string& output, const std::array<T, N>& v);
template <typename T, std::size_t N>
void deserialize(const uint8_t*& start, const uint8_t* end, std::array<T, N>& v);

// KVPairs
template <typename K, typename V>
void serialize(std::vector<uint8_t>& output, const std::pair<K, V>& kvpair);
template <typename K, typename V>
void serialize(std::string& output, const std::pair<K, V>& kvpair);
template <typename K, typename V>
void deserialize(const uint8_t*& start, const uint8_t* end, std::pair<K, V>& kvpair);

// Maps
template <typename K, typename V>
void serialize(std::vector<uint8_t>& output, const std::map<K, V>& m);
template <typename K, typename V>
void serialize(std::string& output, const std::map<K, V>& m);
template <typename K, typename V>
void deserialize(const uint8_t*& start, const uint8_t* end, std::map<K, V>& m);

// Optionals
template <typename T>
void serialize(std::vector<uint8_t>& output, const std::optional<T>& t);
template <typename T>
void serialize(std::string& output, const std::optional<T>& t);
template <typename T>
void deserialize(const uint8_t*& start, const uint8_t* end, std::optional<T>& t);

/******************************************************************************
 * Lists are modeled as std::vectors
 *
 * Lists are preceded by a uint32_t length
 ******************************************************************************/
template <typename T>
void serialize(std::vector<uint8_t>& output, const std::vector<T>& v) {
  cmfAssert(v.size() <= 0xFFFFFFFF);
  uint32_t length = v.size() & 0xFFFFFFFF;
  serialize(output, length);
  for (auto& it : v) {
    serialize(output, it);
  }
}

template <typename T>
void serialize(std::string& output, const std::vector<T>& v) {
  cmfAssert(v.size() <= 0xFFFFFFFF);
  uint32_t length = v.size() & 0xFFFFFFFF;
  serialize(output, length);
  for (auto& it : v) {
    serialize(output, it);
  }
}

template <typename T>
void deserialize(const uint8_t*& start, const uint8_t* end, std::vector<T>& v) {
  uint32_t length;
  deserialize(start, end, length);
  if constexpr (std::is_integral_v<T> && sizeof(T) == 1) {
    // Optimized for bytes
    if (start + length > end) {
      throw NoDataLeftError();
    }
    std::copy_n(start, length, std::back_inserter(v));
    start += length;
  } else {
    for (auto i = 0u; i < length; i++) {
      T t;
      deserialize(start, end, t);
      v.push_back(t);
    }
  }
}

template <typename T, std::size_t N>
void serialize(std::vector<uint8_t>& output, const std::array<T, N>& a) {
  for (auto& it : a) {
    serialize(output, it);
  }
}

template <typename T, std::size_t N>
void serialize(std::string& output, const std::array<T, N>& a) {
  for (auto& it : a) {
    serialize(output, it);
  }
}

template <typename T, std::size_t N>
void deserialize(const uint8_t*& start, const uint8_t* end, std::array<T, N>& a) {
  if constexpr (std::is_integral_v<T> && sizeof(T) == 1) {
    // Optimized for bytes
    if (start + a.size() > end) {
      throw NoDataLeftError();
    }
    std::copy_n(start, a.size(), a.begin());
    start += a.size();
  } else {
    for (auto& t : a) {
      deserialize(start, end, t);
    }
  }
}

/******************************************************************************
 * KVPairs are modeled as std::pairs.
 ******************************************************************************/
template <typename K, typename V>
void serialize(std::vector<uint8_t>& output, const std::pair<K, V>& kvpair) {
  serialize(output, kvpair.first);
  serialize(output, kvpair.second);
}

template <typename K, typename V>
void serialize(std::string& output, const std::pair<K, V>& kvpair) {
  serialize(output, kvpair.first);
  serialize(output, kvpair.second);
}

template <typename K, typename V>
void deserialize(const uint8_t*& start, const uint8_t* end, std::pair<K, V>& kvpair) {
  deserialize(start, end, kvpair.first);
  deserialize(start, end, kvpair.second);
}

/******************************************************************************
 * Maps
 *
 * Maps are preceded by a uint32_t size
 ******************************************************************************/
template <typename K, typename V>
void serialize(std::vector<uint8_t>& output, const std::map<K, V>& m) {
  cmfAssert(m.size() <= 0xFFFFFFFF);
  uint32_t size = m.size() & 0xFFFFFFFF;
  serialize(output, size);
  for (auto& it : m) {
    serialize(output, it);
  }
}

template <typename K, typename V>
void serialize(std::string& output, const std::map<K, V>& m) {
  cmfAssert(m.size() <= 0xFFFFFFFF);
  uint32_t size = m.size() & 0xFFFFFFFF;
  serialize(output, size);
  for (auto& it : m) {
    serialize(output, it);
  }
}

template <typename K, typename V>
void deserialize(const uint8_t*& start, const uint8_t* end, std::map<K, V>& m) {
  uint32_t size;
  deserialize(start, end, size);
  for (auto i = 0u; i < size; i++) {
    std::pair<K, V> kvpair;
    deserialize(start, end, kvpair);
    m.insert(kvpair);
  }
}

/******************************************************************************
 * Optionals are modeled as std::optional
 *
 * Optionals are preceded by a bool indicating whether a value is present or not.
 ******************************************************************************/
template <typename T>
void serialize(std::vector<uint8_t>& output, const std::optional<T>& t) {
  serialize(output, t.has_value());
  if (t.has_value()) {
    serialize(output, t.value());
  }
}

template <typename T>
void serialize(std::string& output, const std::optional<T>& t) {
  serialize(output, t.has_value());
  if (t.has_value()) {
    serialize(output, t.value());
  }
}

template <typename T>
void deserialize(const uint8_t*& start, const uint8_t* end, std::optional<T>& t) {
  bool has_value;
  deserialize(start, end, has_value);
  if (has_value) {
    T value;
    deserialize(start, end, value);
    t = value;
  } else {
    t = std::nullopt;
  }
}

}  // namespace cmf
