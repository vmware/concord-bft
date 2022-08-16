#include <utt/Configuration.h>

#include <optional>

#include <utt/Address.h>
#include <utt/BudgetProof.h>
#include <utt/Coin.h>
#include <utt/Comm.h>
#include <utt/Params.h>
#include <utt/MintOp.h>
#include <utt/RandSig.h>

#include <xassert/XAssert.h>
#include <xutils/Log.h>

std::ostream& operator<<(std::ostream& out, const libutt::MintOp& op) {
  out << op.sn << endl;
  out << op.pidHash << endl;
  out << op.clientId.size() << endl;
  out.write((char*)(op.clientId.data()), (long)op.clientId.size());
  out << op.value;
  return out;
}

std::istream& operator>>(std::istream& in, libutt::MintOp& op) {
  in >> op.sn;
  libff::consume_OUTPUT_NEWLINE(in);
  in >> op.pidHash;
  libff::consume_OUTPUT_NEWLINE(in);
  size_t idSize{0};
  in >> idSize;
  libff::consume_OUTPUT_NEWLINE(in);
  op.clientId.resize(idSize);
  if (idSize > 0) in.read((char*)(op.clientId.data()), (long)idSize);
  in >> op.value;
  return in;
}

namespace libutt {

static Fr createSN(std::string uniqueHash) { return hashToField("new sn|" + uniqueHash); }

MintOp::MintOp(const std::string& uniqueHash, size_t v, const std::string& recipPID) {
  testAssertGreaterThanOrEqual(v, 1);
  clientId = recipPID;
  sn = createSN(uniqueHash);
  pidHash = AddrSK::pidHash(recipPID);
  value.set_ulong(static_cast<unsigned long>(v));
}
MintOp::MintOp(std::istream& in) { in >> *this; }
bool MintOp::validate(const std::string& uniqueHash, size_t v, const std::string& recipPID) const {
  Fr snV = createSN(uniqueHash);
  Fr pidHashV = AddrSK::pidHash(recipPID);
  Fr valueV;
  valueV.set_ulong(static_cast<unsigned long>(v));

  return ((sn == snV) && (pidHash == pidHashV) && (valueV == value));
}

RandSigShare MintOp::shareSignCoin(const RandSigShareSK& bskShare) const {
  std::string h1 = this->getHashHex();
  G1 H = hashToGroup<G1>("ps16base|" + h1);

  Fr coin_type = Coin::NormalType();
  Fr exp_date = Coin::DoesNotExpire();

  Comm icm = (pidHash * H);  // H^pidHash g^0
  Comm scm(sn * H);          // H^sn g^0
  Comm vcm = (value * H);    // H^value g^0
  Comm tcm(coin_type * H);   // H^type g^0
  Comm dcm(exp_date * H);    // H^exp_date g^0

  return bskShare.shareSign({icm, scm, vcm, tcm, dcm}, H);
}

bool MintOp::verifySigShare(const RandSigShare& sigShare, const RandSigSharePK& bpkShare) const {
  std::string h1 = this->getHashHex();
  G1 H = hashToGroup<G1>("ps16base|" + h1);

  Fr coin_type = Coin::NormalType();
  Fr exp_date = Coin::DoesNotExpire();

  Comm icm = (pidHash * H);  // H^pidHash g^0
  Comm scm(sn * H);          // H^sn g^0
  Comm vcm = (value * H);    // H^value g^0
  Comm tcm(coin_type * H);   // H^type g^0
  Comm dcm(exp_date * H);    // H^exp_date g^0

  return sigShare.verify({icm, scm, vcm, tcm, dcm}, bpkShare);
}

Coin MintOp::claimCoin(const Params& p,
                       const AddrSK& ask,
                       size_t n,
                       const std::vector<RandSigShare>& sigShares,
                       const std::vector<size_t>& signerIds,
                       const RandSigPK& bpk) const {
  assertTrue(pidHash == ask.getPidHash());

  Fr r_pid = Fr::zero(), r_sn = Fr::zero(), r_val = Fr::zero(), r_type = Fr::zero(), r_expdate = Fr::zero();

  std::vector<Fr> r = {r_pid, r_sn, r_val, r_type, r_expdate};

  testAssertFalse(sigShares.empty());
  testAssertFalse(signerIds.empty());
  testAssertEqual(sigShares.size(), signerIds.size());

  RandSig sig = RandSigShare::aggregate(n, sigShares, signerIds, p.getCoinCK(), r);

  Fr coin_type = Coin::NormalType();
  Fr exp_date = Coin::DoesNotExpire();

#ifndef NDEBUG
  {
    Comm ccm = Comm::create(p.getCoinCK(),
                            {
                                ask.getPidHash(),
                                sn,
                                value,
                                coin_type,
                                exp_date,
                                Fr::zero()  // the recovered signature will be on a commitment w/ randomness 0
                            },
                            true);

    assertTrue(sig.verify(ccm, bpk));
  }
#endif

  Coin c(p.getCoinCK(), p.null, sn, value, coin_type, exp_date, ask);

  // re-randomize the coin signature
  Fr u_delta = Fr::random_element();
  c.sig = sig;
  c.sig.rerandomize(c.r, u_delta);

  assertTrue(c.hasValidSig(bpk));
  assertNotEqual(c.r, Fr::zero());  // should output a re-randomized coin always

  return c;
}

std::string MintOp::getHashHex() const {
  std::stringstream ss;
  ss << *this;
  return hashToHex("MintOp|" + ss.str());
}

}  // namespace libutt
