#include <utt/Configuration.h>

#include <optional>

#include <utt/Address.h>
#include <utt/Coin.h>
#include <utt/Factory.h>
#include <utt/Params.h>
#include <utt/RandSig.h>
#include <utt/RandSigDKG.h>
#include <utt/RegAuth.h>
#include <utt/BudgetProof.h>
#include <utt/Comm.h>
#include <utt/Tx.h>
#include <utt/BurnOp.h>

#include <utt/Serialization.h>

#include <xassert/XAssert.h>
#include <xutils/Log.h>
#include <xutils/Utils.h>
struct InternalDataOfBurnOp {
  std::string pid;   // owner pid
  libutt::Fr value;  // value of the burned coin
  libutt::Fr r_d;    // randomness for vcm_2 of the burned coin
  libutt::Fr r_t;    // randomness for 'icm' of the burned coin
  libutt::Tx tx;
};

std::ostream& operator<<(std::ostream& out, const libutt::BurnOp& op) {
  InternalDataOfBurnOp* d = (InternalDataOfBurnOp*)op.p;
  out << (d->pid) << endl;
  out << (d->value) << endl;
  out << (d->r_d) << endl;
  out << (d->r_t) << endl;
  ::operator<<(out, d->tx);

  return out;
}

std::istream& operator>>(std::istream& in, libutt::BurnOp& op) {
  InternalDataOfBurnOp* d = (InternalDataOfBurnOp*)op.p;

  if (d != nullptr) delete d;

  d = new InternalDataOfBurnOp;

  in >> d->pid;
  libff::consume_OUTPUT_NEWLINE(in);
  in >> d->value;
  libff::consume_OUTPUT_NEWLINE(in);
  in >> d->r_d;
  libff::consume_OUTPUT_NEWLINE(in);
  in >> d->r_t;
  libff::consume_OUTPUT_NEWLINE(in);
  ::operator>>(in, d->tx);

  op.p = d;

  return in;
}

namespace libutt {

BurnOp::BurnOp(std::istream& in) : p(nullptr) { in >> *this; }

BurnOp::BurnOp(const Params& p, const AddrSK& ask, const Coin& coin, const RandSigPK& bpk, const RegAuthPK& rpk)
    : BurnOp{p, ask.pid_hash, ask.pid, ask.rcm, ask.rs, ask.s, coin, bpk, rpk} {}
BurnOp::BurnOp(const Params& p,
               const Fr pidHash,
               const std::string& pid,
               const Comm& rcm_,
               const RandSig& rcm_sig,
               const Fr& prf,
               const Coin& coin,
               std::optional<RandSigPK> bpk,
               const RegAuthPK& rpk) {
  InternalDataOfBurnOp* d = new InternalDataOfBurnOp;
  this->p = d;

  d->pid = pid;
  d->value = coin.val;

  std::vector<Coin> inputCoins = std::vector<Coin>{coin};

  std::vector<std::tuple<std::string, Fr>> recip;
  recip.emplace_back(pid, d->value);
  Tx internalTx(p, pidHash, pid, rcm_, rcm_sig, prf, inputCoins, std::nullopt, recip, bpk, rpk.vk, rpk.mpk);

  assertTrue(internalTx.outs.size() == 1);

  d->r_d = internalTx.outs[0].d;
  d->r_t = internalTx.outs[0].t;

  // TODO: improve
  //  d->tx = internalTx;
  std::stringstream ss;
  ::operator<<(ss, internalTx);
  ::operator>>(ss, d->tx);

  assertTrue(internalTx.getHashHex() == d->tx.getHashHex());
}

BurnOp::BurnOp(const BurnOp& o) {
  if (o.p == nullptr) return;
  InternalDataOfBurnOp* d1 = new InternalDataOfBurnOp;
  this->p = d1;
  InternalDataOfBurnOp* d2 = (InternalDataOfBurnOp*)o.p;
  d1->pid = d2->pid;
  d1->value = d2->value;
  d1->r_d = d2->r_d;
  d1->r_t = d2->r_t;

  // TODO: improve
  // d1->tx = d2->tx;
  std::stringstream ss;
  ::operator<<(ss, d2->tx);
  ::operator>>(ss, d1->tx);

  assertTrue(d1->tx.getHashHex() == d2->tx.getHashHex());
}

BurnOp::~BurnOp() {
  InternalDataOfBurnOp* d = (InternalDataOfBurnOp*)this->p;
  if (d != nullptr) delete d;
}

BurnOp& BurnOp::operator=(const BurnOp& o) {
  InternalDataOfBurnOp* d1 = (InternalDataOfBurnOp*)this->p;

  if (d1 != nullptr) {
    delete d1;
    this->p = nullptr;
  }

  InternalDataOfBurnOp* d2 = (InternalDataOfBurnOp*)o.p;

  if (d2 != nullptr) {
    d1 = new InternalDataOfBurnOp;
    this->p = d1;

    d1->pid = d2->pid;
    d1->value = d2->value;
    d1->r_d = d2->r_d;
    d1->r_t = d2->r_t;

    // TODO: improve
    // d1->tx = d2->tx;
    std::stringstream ss;
    ::operator<<(ss, d2->tx);
    ::operator>>(ss, d1->tx);
    assertTrue(d1->tx.getHashHex() == d2->tx.getHashHex());
  }

  return *this;
}

size_t BurnOp::getSize() const {
  InternalDataOfBurnOp* d = (InternalDataOfBurnOp*)this->p;

  if (d == nullptr) return 0;

  const size_t pidSize = d->pid.length();
  const size_t internalTxSize = d->tx.getSize();

  return (_fr_size * 3 + pidSize + internalTxSize);
}

bool BurnOp::validate(const Params& p, const RandSigPK& bpk, const RegAuthPK& rpk) const {
  InternalDataOfBurnOp* d = (InternalDataOfBurnOp*)this->p;
  if (d == nullptr) return false;
  if (!d->tx.validate(p, bpk, rpk)) return false;
  if (d->tx.outs.size() != 1) return false;

  Fr hashPid = AddrSK::pidHash(d->pid);

  CommKey ck_val = p.getValCK();

  G1 H;

  if (d->tx.outs[0].H.has_value()) {
    H = *d->tx.outs[0].H;
  } else {
    H = d->tx.deriveRandSigBase(0);
  }

  CommKey ck_tx = CommKey({H, ck_val.getGen1()});

  Comm temp_icm = Comm::create(ck_tx, {hashPid, d->r_t}, false);
  Comm temp_vcm = Comm::create(ck_tx, {d->value, d->r_d}, false);

  TxOut& outTran = d->tx.outs[0];

  return (temp_icm.ped1 == outTran.icm.ped1) && (temp_vcm.ped1 == outTran.vcm_2.ped1);
}

size_t BurnOp::getValue() const {
  InternalDataOfBurnOp* d = (InternalDataOfBurnOp*)this->p;
  assertTrue(d != nullptr);
  size_t v = static_cast<size_t>(d->value.as_ulong());
  assertTrue(v > 0);
  return v;
}

std::string BurnOp::getOwnerPid() const {
  InternalDataOfBurnOp* d = (InternalDataOfBurnOp*)this->p;
  assertTrue(d != nullptr);
  return d->pid;
}

std::string BurnOp::getNullifier() const {
  InternalDataOfBurnOp* d = (InternalDataOfBurnOp*)this->p;
  assertTrue(d != nullptr);
  std::vector<std::string> nullsArray = d->tx.getNullifiers();
  assertTrue(nullsArray.size() == 1);
  return nullsArray[0];
}

std::string BurnOp::getHashHex() const {
  std::stringstream ss;
  ss << *this;
  return hashToHex("BurnOp|" + ss.str());
}

bool BurnOp::operator==(const BurnOp& o) const {
  if (this->p != o.p)
    return false;
  else if (this->p == nullptr)
    return true;
  else {
    InternalDataOfBurnOp* d1 = (InternalDataOfBurnOp*)this->p;
    InternalDataOfBurnOp* d2 = (InternalDataOfBurnOp*)o.p;

    return (d1->pid == d2->pid) && (d1->value == d2->value) && (d1->r_d == d2->r_d) && (d1->r_t == d2->r_t) &&
           (d1->tx == d2->tx);
  }
}

}  // namespace libutt
