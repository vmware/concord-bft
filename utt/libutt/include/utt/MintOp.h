#pragma once

#include <cstddef>
#include <optional>
#include <vector>
#include <xassert/XAssert.h>
#include <xutils/Log.h>
#include <utt/PolyCrypto.h>
namespace libutt {
class Coin;
class Params;
class MintOp;
class RandSigShare;
class RandSigShareSK;
class RandSigSharePK;
class AddrSK;
class RandSigPK;
}  // namespace libutt

std::ostream& operator<<(std::ostream&, const libutt::MintOp&);
std::istream& operator>>(std::istream&, libutt::MintOp&);

namespace libutt {

// Represents an operation that creates a new coin. This operation should be
// part of a public transaction (e.g., a transaction that converts public money
// to a new anonymous coin).
class MintOp {
 protected:
  Fr sn;       // serial number of the new coin
  Fr pidHash;  // hash of recipient pid
  Fr value;    // value of the new coin
  std::string clientId;
  // Notice that all data members are public because we assume that creating an anonymous new coin should be part of a
  // public transaction
 public:
  friend std::ostream& ::operator<<(std::ostream&, const libutt::MintOp&);
  friend std::istream& ::operator>>(std::istream&, libutt::MintOp&);
  // uniqueHash should depend on the full transaction that contains this action.
  // NB: In order to make sure that the value of uniqueHash is unique it is possible to add a random value
  // to the transaction
  MintOp(const std::string& uniqueHash, size_t value, const std::string& recipPID);

  MintOp(std::istream& in);
  const std::string& getClientId() const { return clientId; }
  size_t getSize() const { return _fr_size * 3; }

  // uniqueHash, value and recipPID should be taken from the public transaction
  // NB: a possible way to prevent non-unique values of uniqueHash is to also verify that the new serial number was
  // never used in the past (in such a case, the replicas should not sign the new coin)
  bool validate(const std::string& uniqueHash, size_t value, const std::string& recipPID) const;

  RandSigShare shareSignCoin(const RandSigShareSK& bskShare) const;

  bool verifySigShare(const RandSigShare& sigShare, const RandSigSharePK& bpkShare) const;

  Coin claimCoin(const Params& p,
                 const AddrSK& ask,
                 size_t n,
                 const std::vector<RandSigShare>& sigShares,
                 const std::vector<size_t>& signerIds,
                 const RandSigPK& bpk) const;

  std::string getHashHex() const;

  Fr getSN() { return sn; }
  Fr getVal() { return value; }
  bool operator==(const MintOp& o) const { return ((sn == o.sn) && (pidHash == o.pidHash) && (value == o.value)); }
  bool operator!=(const MintOp& o) const { return !operator==(o); }
};

}  // end of namespace libutt