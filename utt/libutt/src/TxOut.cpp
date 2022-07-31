#include <utt/Configuration.h>

#include <optional>

#include <utt/Address.h>
#include <utt/Coin.h>
#include <utt/Params.h>
#include <utt/TxOut.h>
#include <utt/DataUtils.hpp>
#include <utt/Serialization.h>  // WARNING: include last

std::ostream& operator<<(std::ostream& out, const libutt::TxOut& txout) {
  out << txout.coin_type << endl;
  out << txout.exp_date << endl;

  // out << txout.ck_tx;

  out << txout.vcm_1;
  out << txout.range_pi;

  // out << txout.d << endl;
  out << txout.vcm_2;
  out << txout.vcm_eq_pi;

  // out << txout.t << endl;
  out << txout.icm;
  out << txout.icm_pok;

  out << std::string(txout.ctxt.begin(), txout.ctxt.end()) << endl;
  return out;
}

std::istream& operator>>(std::istream& in, libutt::TxOut& txout) {
  in >> txout.coin_type;
  libff::consume_OUTPUT_NEWLINE(in);
  in >> txout.exp_date;
  libff::consume_OUTPUT_NEWLINE(in);

  // in >> txout.ck_tx;

  in >> txout.vcm_1;
  in >> txout.range_pi;

  // in >> txout.d;
  // libff::consume_OUTPUT_NEWLINE(in);
  in >> txout.vcm_2;
  in >> txout.vcm_eq_pi;

  // in >> txout.t;
  // libff::consume_OUTPUT_NEWLINE(in);
  in >> txout.icm;
  in >> txout.icm_pok;
  std::string s;
  in >> s;
  memcpy(txout.ctxt.data(), s.data(), s.size());
  libff::consume_OUTPUT_NEWLINE(in);
  return in;
}

namespace libutt {

TxOut::TxOut(const CommKey& ck_val,
             const RangeProof::Params& rpp,
             const Fr& coin_type,
             const Fr& exp_date,
             const G1& H,
             const std::string& pid,
             const Fr& val,
             const Fr& z,
             bool icmPok,
             bool hasRangeProof,
             const IEncryptor& encryptor)
    : TxOut(ck_val,
            rpp,
            coin_type,
            exp_date,
            H,
            pid,
            val,
            z,
            icmPok,
            hasRangeProof,
            // extra delegation parameters
            CommKey({H, ck_val.getGen1()}),
            AddrSK::pidHash(pid),
            encryptor) {}

TxOut::TxOut(const CommKey& ck_val,  // this is the (g_3, g) CK, and *not* the coin CK
             const RangeProof::Params& rpp,
             const Fr& coin_type,
             const Fr& exp_date,
             const G1& H,
             const std::string& pid,
             const Fr& val,
             const Fr& z,
             bool icmPok,
             bool hasRangeProof,
             // extra delegation parameters
             CommKey ck_tx,
             Fr pid_hash_recip,
             const IEncryptor& encryptor)
    : coin_type(coin_type),
      exp_date(exp_date),
      val(val),  // not serialized, only for claiming the coin
      H(H),      // cache it here, so clients can verify their sigshares faster
      // z(z),
      vcm_1(Comm::create(ck_val, {val, z}, false)),
      // need to pick randomness d for vcm_2 and save it to do proofs
      d(Fr::random_element()),
      vcm_2(Comm::create(ck_tx, {val, d}, false)),
      vcm_eq_pi(ck_val, vcm_1, z, ck_tx, vcm_2, d, val),
      // need to pick randomness t for icm and save it to do proofs
      t(Fr::random_element()),
      icm(Comm::create(ck_tx, {pid_hash_recip, t}, false)),
      ctxt(encryptor.encrypt(pid, frsToBytes({val, d, t}))) {
  if (icmPok) {
    icm_pok.emplace(ck_tx, icm, pid_hash_recip, t);
  }

  if (hasRangeProof) {
    range_pi.emplace(rpp, vcm_1, val, z);
  }

  logtrace << "val: " << val << endl;
  logtrace << "d: " << d << endl;
  logtrace << "t: " << t << endl;
}

std::string TxOut::hash() const {
  std::stringstream ss;
  ss << *this;
  return picosha2::hash256_hex_string(ss.str());
}

std::string TxOut::hashAll(const std::vector<TxOut>& txouts) {
  std::string hashes;
  if (txouts.size() == 0) throw std::runtime_error("Need more than one TxOut to hash");

  hashes = txouts[0].hash();

  if (txouts.size() > 1) {
    for (size_t i = 0; i < txouts.size() - 1; i++) {
      hashes += "|" + txouts[i].hash();
    }
  }

  return picosha2::hash256_hex_string(hashes);
}

}  // end of namespace libutt
