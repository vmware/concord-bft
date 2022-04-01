#include <utt/Configuration.h>

#include <utt/Address.h>
#include <utt/Nullifier.h>

// WARNING: Define (de)serialization stream operators in the *global* namespace
std::ostream& operator<<(std::ostream& out, const libutt::Nullifier& null) {
  out << null.n << endl;
  out << null.y << endl;
  out << null.vk << endl;
  return out;
}

// WARNING: Define (de)serialization stream operators in the *global* namespace
std::istream& operator>>(std::istream& in, libutt::Nullifier& null) {
  in >> null.n;
  libff::consume_OUTPUT_NEWLINE(in);

  in >> null.y;
  libff::consume_OUTPUT_NEWLINE(in);

  in >> null.vk;
  libff::consume_OUTPUT_NEWLINE(in);
  return in;
}

// WARNING: Define (de)serialization stream operators in the *global* namespace
std::ostream& operator<<(std::ostream& out, const libutt::Nullifier::Params& null) {
  out << null.h << endl;
  out << null.h_tilde << endl;
  out << null.w_tilde << endl;
  return out;
}

// WARNING: Define (de)serialization stream operators in the *global* namespace
std::istream& operator>>(std::istream& in, libutt::Nullifier::Params& null) {
  in >> null.h;
  libff::consume_OUTPUT_NEWLINE(in);

  in >> null.h_tilde;
  libff::consume_OUTPUT_NEWLINE(in);

  in >> null.w_tilde;
  libff::consume_OUTPUT_NEWLINE(in);

  null.ehh = libutt::ReducedPairing(null.h, null.h_tilde);
  return in;
}

namespace libutt {

Nullifier::Nullifier(const Params& p, const AddrSK& ask, const Fr& sn, const Fr& t)
    : n((ask.s + sn).inverse() * p.h),
      y(ReducedPairing(n, p.w_tilde) ^ t),
      vk((ask.s + sn) * p.h_tilde + t * p.w_tilde) {
  assertTrue(Nullifier::verify(p));
}

bool Nullifier::verify(const Params& p) const { return ReducedPairing(n, vk) == p.ehh * y; }

}  // namespace libutt
