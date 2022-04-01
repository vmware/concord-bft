#pragma once

#include <optional>

#include <utt/Comm.h>
#include <utt/IBE.h>
#include <utt/PolyCrypto.h>
#include <utt/RandSig.h>

#include <xassert/XAssert.h>
#include <xutils/Log.h>

namespace libutt {
class AddrSK;
}

std::ostream& operator<<(std::ostream& out, const libutt::AddrSK& ask);
std::istream& operator>>(std::istream& in, libutt::AddrSK& ask);

namespace libutt {

class AddrSK {
 public:
  // the public identifier of this user, e.g., alice@wonderland.com
  std::string pid;

  // the public identifier, as a hash of the pid string into a field element
  Fr pid_hash;

  // PRF secret key
  Fr s;

  // Encryption secret key, derived from the IBE MSK (used for Diffie-Hellman key exchange + AES)
  IBE::EncSK e;

  // Registration commitment to (pid, s), with randomness **0**, but always re-randomized during a TXN
  Comm rcm;

  // Registration signature on rcm
  RandSig rs;

 public:
  std::string getPid() const { return pid; }
  Fr getPidHash() const;

 public:
  static Fr pidHash(const std::string& pid);

 public:
  bool operator==(const AddrSK& o) const {
    return pid == o.pid && pid_hash == o.pid_hash && s == o.s && e == o.e && rcm == o.rcm && rs == o.rs && true;
  }

  bool operator!=(const AddrSK& o) const { return !operator==(o); }
};
}  // namespace libutt
