#include <utt/Configuration.h>

#include <sstream>

#include <utt/Address.h>

#include <libfqfft/polynomial_arithmetic/basic_operations.hpp>

#include <xutils/NotImplementedException.h>

std::ostream& operator<<(std::ostream& out, const libutt::AddrSK& ask) {
  out << ask.pid << endl;
  out << ask.pid_hash << endl;
  out << ask.s << endl;
  out << ask.e << endl;
  out << ask.rcm << endl;
  out << ask.rs << endl;
  return out;
}

std::istream& operator>>(std::istream& in, libutt::AddrSK& ask) {
  in >> ask.pid;
  libff::consume_OUTPUT_NEWLINE(in);
  in >> ask.pid_hash;
  libff::consume_OUTPUT_NEWLINE(in);
  testAssertEqual(ask.pid_hash, libutt::AddrSK::pidHash(ask.pid));
  in >> ask.s;
  libff::consume_OUTPUT_NEWLINE(in);
  in >> ask.e;
  libff::consume_OUTPUT_NEWLINE(in);
  in >> ask.rcm;
  libff::consume_OUTPUT_NEWLINE(in);
  in >> ask.rs;
  libff::consume_OUTPUT_NEWLINE(in);
  return in;
}

namespace libutt {

Fr AddrSK::getPidHash() const { return pid_hash; }

Fr AddrSK::pidHash(const std::string& pid) {
  // TODO(Crypto): See libutt/hashing.md
  return hashToField(pid);
}

}  // namespace libutt
