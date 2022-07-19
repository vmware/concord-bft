#include <utt/Configuration.h>

#include <istream>
#include <libff/common/serialization.hpp>
#include <memory>
#include <ostream>

#include <utt/Address.h>
#include <utt/Coin.h>
#include <utt/Comm.h>
#include <utt/Nullifier.h>
#include <utt/PolyCrypto.h>

#include <utt/Serialization.h>  // WARNING: Include this last (see header file for details; thanks, C++)

// WARNING: Must be defined in the global namespace here!
std::ostream& operator<<(std::ostream& out, const libutt::Coin& c) {
  out << c.ck << endl;
  out << c.pid_hash << endl;
  out << c.sn << endl;
  out << c.val << endl;
  out << c.type << endl;
  out << c.exp_date << endl;
  out << c.r << endl;
  out << c.sig << endl;
  out << c.t << endl;
  out << c.null << endl;
  out << c.vcm << endl;
  out << c.z << endl;
  out << c.ccm_txn << endl;
  return out;
}

// WARNING: Must be defined in the global namespace here!
std::istream& operator>>(std::istream& in, libutt::Coin& c) {
  in >> c.ck;
  libff::consume_OUTPUT_NEWLINE(in);

  in >> c.pid_hash;
  libff::consume_OUTPUT_NEWLINE(in);

  in >> c.sn;
  libff::consume_OUTPUT_NEWLINE(in);

  in >> c.val;
  libff::consume_OUTPUT_NEWLINE(in);

  in >> c.type;
  libff::consume_OUTPUT_NEWLINE(in);

  in >> c.exp_date;
  libff::consume_OUTPUT_NEWLINE(in);

  in >> c.r;
  libff::consume_OUTPUT_NEWLINE(in);

  in >> c.sig;
  libff::consume_OUTPUT_NEWLINE(in);

  in >> c.t;
  libff::consume_OUTPUT_NEWLINE(in);

  in >> c.null;
  libff::consume_OUTPUT_NEWLINE(in);

  in >> c.vcm;
  libff::consume_OUTPUT_NEWLINE(in);

  in >> c.z;
  libff::consume_OUTPUT_NEWLINE(in);

  in >> c.ccm_txn;
  libff::consume_OUTPUT_NEWLINE(in);

  return in;
}

namespace libutt {
Coin::Coin(const CommKey& ck, const Fr& sn, const Fr& val, const Fr& type, const Fr& exp_date, const Fr& pidHash)
    : ck(ck), pid_hash(pidHash), sn(sn), val(val), type(type), exp_date(exp_date), t(Fr::random_element()) {
  Coin::commit();
}

Coin::Coin(const CommKey& ck,
           const Nullifier::Params& np,
           const Fr& sn,
           const Fr& val,
           const Fr& type,
           const Fr& exp_date,
           const AddrSK& ask)
    : ck(ck), pid_hash(ask.getPidHash()), sn(sn), val(val), type(type), exp_date(exp_date), t(Fr::random_element()) {
  // computes the partial commitment, which is what we include in a TXN
  Coin::commit();

  // pre-compute the coin's nullifier
  null = Nullifier(np, ask, sn, t);
}

bool Coin::hasValidSig(const RandSigPK& pk) const { return sig.verify(augmentComm(), pk); }

Comm Coin::augmentComm(const CommKey& ck, const Comm& ccmTxn, const Fr& type, const Fr& exp_date) {
  // WARNING: Hardcoding this for now, since the code below assumes it
  // TODO(Safety): better ways of computing "subcommitment keys"
  assertTrue(ck.hasCorrectG2());
  assertEqual(ck.numMessages(), 5);

  CommKey ck_extra;
  ck_extra.g.push_back(ck.g[3]);  // g_4
  ck_extra.g.push_back(ck.g[4]);  // g_5
  ck_extra.g_tilde.push_back(ck.g_tilde[3]);
  ck_extra.g_tilde.push_back(ck.g_tilde[4]);
  assertTrue(ck_extra.hasCorrectG2());

  // need both G1 and G2 counterparts, since this will be used for signature verification
  // logdbg << "Committing with CK: " << ck_extra << endl;
  Comm ccmExtra = Comm::create(ck_extra, {type, exp_date}, true);

  return ccmTxn + ccmExtra;
}

void Coin::commit() {
  assertGreaterThanOrEqual(ck.numMessages(), 3);
  assertTrue(ck.hasCorrectG2());

  // we first commit *partially* to the coin, but not to its type and expiration date, which will be given in plaintext
  bool withG2 = true;  // since we always need G2 counterparts to verify sigs
  r = Fr::random_element();
  ccm_txn = Comm::create(ck, {pid_hash, sn, val, Fr::zero(), Fr::zero(), r}, withG2);
  assertTrue(ccm_txn.hasCorrectG2(ck));

  // NOTE: This is a bit hard-coded. Ideally, we might've done ck_val = Params::getValCK()?
  // But that couples Coin with Params. Maybe this is okay after all.

  // set the vcm commitment key to (g_3, g)
  CommKey ck_val;
  ck_val.g.push_back(ck.g[2]);       // g_3
  ck_val.g.push_back(ck.getGen1());  // g

  // pick randomness and compute value commitment
  z = Fr::random_element();
  vcm = Comm::create(ck_val, {val, z}, false);
}

}  // end of namespace libutt
