#pragma once

#include <utt/PolyCrypto.h>

#include <xassert/XAssert.h>
#include <xutils/Log.h>

namespace libutt {
class PedEqProof;
class ZKPoK;
}  // namespace libutt

std::ostream& operator<<(std::ostream&, const libutt::PedEqProof&);
std::istream& operator>>(std::istream&, libutt::PedEqProof&);

std::ostream& operator<<(std::ostream&, const libutt::ZKPoK&);
std::istream& operator>>(std::istream&, libutt::ZKPoK&);

namespace libutt {

class Comm;
class CommKey;

/**
 * Proof that two Pedersen commitments g_1^m g^r_1 and h_1^m g^r_2 have the same message m
 */
class PedEqProof {
 public:
  std::vector<Fr> s;
  Fr e;

 public:
  size_t getSize() const { return sizeof(size_t) + s.size() * _fr_size + _fr_size; }
  PedEqProof() {}

  PedEqProof(const CommKey& ck1,
             const Comm& cm1,
             const Fr& r_1,
             const CommKey& ck2,
             const Comm& cm2,
             const Fr& r_2,
             const Fr& m);

 public:
  bool verify(const CommKey& ck1, const Comm& cm1, const CommKey& ck2, const Comm& cm2) const;

  Fr hash(const CommKey& ck1, const Comm& cm1, const CommKey& ck2, const Comm& cm2, const std::vector<G1>& X) const;
};

/**
 * ZKPoK of (m, t) w.r.t. Pedersen commitment g_1^m g^t under CK (g_1, g)
 */
class ZKPoK {
 public:
  Fr s_m, s_t;
  Fr e;

 public:
  size_t getSize() const { return _fr_size + _fr_size + _fr_size; }

  ZKPoK() {}

  ZKPoK(std::istream& in) : ZKPoK() { in >> *this; }

  ZKPoK(const CommKey& ck, const Comm& cm, const Fr& m, const Fr& t);

 public:
  bool verify(const CommKey& ck, const Comm& cm) const;

  Fr hash(const CommKey& ck, const Comm& cm, const G1& R) const;
};

}  // end of namespace libutt
