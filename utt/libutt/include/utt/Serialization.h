/**
 * WARNING: It seems like if I were to put these template functions in a file like PolyCrypto.h that
 * gets #include'd all over the place C++, naturally, borks for no reason.
 *
 * To the best of my understanding, the reason it borks is because these template<T>'s end up being declared *before*
 * the compiler gets to see a declaration for, say, 'operator<<(ostream&, const T&)', due to the
 * ordering of #include's.
 *
 * So what I've done is moved these into their own file, which I am sure to include at the very end of my .cpp files.
 */
#pragma once

#include <iostream>
#include <optional>

#include <libff/common/serialization.hpp>
// using libff::operator<<;
// using libff::operator>>;

#include <xutils/Log.h>

using std::endl;

/**
 * We (de)serialize a lot of std::vector<T>'s and std::optional<T>'s.
 * It's getting boring.
 */
template <class T>
std::ostream& operator<<(std::ostream& out, const std::optional<T>& opt) {
  if (opt.has_value()) {
    out << true << endl;
    out << *opt << endl;
  } else {
    out << false << endl;
  }
  return out;
}

template <class T>
std::istream& operator>>(std::istream& in, std::optional<T>& opt) {
  bool hasVal;
  in >> hasVal;
  libff::consume_OUTPUT_NEWLINE(in);

  if (hasVal) {
    T val;
    in >> val;
    opt = val;
    libff::consume_OUTPUT_NEWLINE(in);
  }
  return in;
}

/**
 * Actually, vector (de)serialization is already implemented in #include <libff/common/serialization.hpp>,
 * but it is namespaced in libff and cannot for the life of me figure out the compiling errors.
 *
 * The way C++ looks up templated namespaced operators is completely primitive.
 *
 * I tried 'using operator<<;' after #include'ing the libff file above, in order
 * to get the std::vector<T> operators to work but that seems to then make my normal
 * T operator definitions not be found.
 */
// namespace libutt {
//
//    template<class T>
//    std::ostream& operator<<(std::ostream& out, const std::vector<T>& v) {
//        out << v.size() << endl;
//        for(const auto& e : v) {
//            out << e << endl;
//        }
//        return out;
//    }
//
//    template<class T>
//    std::istream& operator>>(std::istream& in, std::vector<T>& v) {
//        size_t len;
//        in >> len;
//        libff::consume_OUTPUT_NEWLINE(in);
//
//        v.resize(len);
//        for(size_t i = 0; i < len; i++) {
//            in >> v[i];
//            libff::consume_OUTPUT_NEWLINE(in);
//        }
//        return in;
//    }
//
//}

namespace libutt {

// thanks, C++!
template <class T>
void serializeVector(std::ostream& out, const std::vector<T>& v) {
  out << v.size() << endl;
  for (const auto& e : v) {
    out << e << endl;
  }
}

template <class T>
void deserializeVector(std::istream& in, std::vector<T>& v) {
  size_t len;
  in >> len;
  libff::consume_OUTPUT_NEWLINE(in);

  v.resize(len);
  for (size_t i = 0; i < len; i++) {
    in >> v[i];
    libff::consume_OUTPUT_NEWLINE(in);
  }
}

}  // namespace libutt
