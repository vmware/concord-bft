#include <istream>
#include <libff/common/serialization.hpp>
#include <memory>
#include <ostream>

#include <utt/Bank.h>
#include <utt/Coin.h>
#include <utt/RangeProof.h>
#include <utt/Utt.h>
#include <utt/Serialization.h>

using namespace std;

namespace libutt {

// Serialization/Deserialization for BankSK
void libutt::BankSK::write(std::ostream& out) const { out << this->x << endl; }

void libutt::BankSK::read(std::istream& in) {
  in >> this->x;
  libff::consume_OUTPUT_NEWLINE(in);
}

std::ostream& operator<<(std::ostream& out, const libutt::BankSK& sk) {
  sk.write(out);
  return out;
}

std::istream& operator>>(std::istream& in, libutt::BankSK& sk) {
  sk.read(in);
  return in;
}

libutt::BankSK::BankSK(std::istream& in) : BankSK() { this->read(in); }

// Serialization/Deserialization for BankShareSK
void libutt::BankShareSK::write(std::ostream& out) const {
  out << this->gToU << endl;
  out << this->shareU << endl;
  this->BankSK::write(out);
}

void libutt::BankShareSK::read(std::istream& in) {
  in >> this->gToU;
  libff::consume_OUTPUT_NEWLINE(in);
  in >> this->shareU;
  libff::consume_OUTPUT_NEWLINE(in);
  this->BankSK::read(in);
}

std::ostream& operator<<(std::ostream& out, const libutt::BankShareSK& shareSK) {
  shareSK.write(out);
  return out;
}

std::istream& operator>>(std::istream& in, libutt::BankShareSK& shareSK) {
  shareSK.read(in);
  return in;
}

libutt::BankShareSK::BankShareSK(std::istream& in) : BankShareSK() { this->read(in); }

// Serialization/Deserialization for BankPK
void libutt::BankPK::write(std::ostream& out) const { out << this->X << endl; }

void libutt::BankPK::read(std::istream& in) {
  in >> this->X;
  libff::consume_OUTPUT_NEWLINE(in);
}

std::ostream& operator<<(std::ostream& out, const libutt::BankPK& bpk) {
  bpk.write(out);
  return out;
}

std::istream& operator>>(std::istream& in, libutt::BankPK& bpk) {
  bpk.read(in);
  return in;
}

libutt::BankPK::BankPK(std::istream& in) : BankPK() { this->read(in); }

// Serialization/Deserialization for BankSharePK
// read and write will be used from the parent class
void libutt::BankSharePK::read(std::istream& in) { this->BankPK::read(in); }

void libutt::BankSharePK::write(std::ostream& out) const { this->BankPK::write(out); }

std::ostream& operator<<(std::ostream& out, const libutt::BankSharePK& sharePK) {
  sharePK.write(out);
  return out;
}

std::istream& operator>>(std::istream& in, libutt::BankSharePK& sharePK) {
  sharePK.read(in);
  return in;
}

libutt::BankSharePK::BankSharePK(std::istream& in) : BankPK(in) {}

// Serialization/Deserialization for CoinSig
void libutt::CoinSig::write(std::ostream& out) const {
  out << this->s1 << endl;
  out << this->s2 << endl;
}

void libutt::CoinSig::read(std::istream& in) {
  in >> this->s1;
  libff::consume_OUTPUT_NEWLINE(in);

  in >> this->s2;
  libff::consume_OUTPUT_NEWLINE(in);
}

std::istream& operator>>(std::istream& in, libutt::CoinSig& csig) {
  csig.read(in);
  return in;
}

std::ostream& operator<<(std::ostream& out, const libutt::CoinSig& csig) {
  csig.write(out);
  return out;
}

libutt::CoinSig::CoinSig(std::istream& in) { this->read(in); }

// Serialization/Deserialization for CoinSigShare
void libutt::CoinSigShare::write(std::ostream& out) const {
  out << this->s1 << endl;
  out << this->s2 << endl;
  out << this->s3 << endl;
}

void libutt::CoinSigShare::read(std::istream& in) {
  in >> this->s1;
  libff::consume_OUTPUT_NEWLINE(in);

  in >> this->s2;
  libff::consume_OUTPUT_NEWLINE(in);

  in >> this->s3;
  libff::consume_OUTPUT_NEWLINE(in);
}

std::istream& operator>>(std::istream& in, libutt::CoinSigShare& csigs) {
  csigs.read(in);
  return in;
}

std::ostream& operator<<(std::ostream& out, const libutt::CoinSigShare& csigs) {
  csigs.write(out);
  return out;
}

libutt::CoinSigShare::CoinSigShare(std::istream& in) { this->read(in); }

// Serialization/Deserialization
void libutt::RangeProof::write(std::ostream& out) const {
  (void)out;
  // After range proof is implemented, add content here
}

void libutt::RangeProof::read(std::istream& in) {
  (void)in;
  // After range proof is implemented, add content here
}

std::ostream& operator<<(std::ostream& out, const libutt::RangeProof& rp) {
  rp.write(out);
  return out;
}

std::istream& operator>>(std::istream& in, libutt::RangeProof& rp) {
  rp.read(in);
  return in;
}

libutt::RangeProof::RangeProof(std::istream& in) { this->read(in); }

// Serialization/Deserialization for SplitProof
void libutt::SplitProof::write(std::ostream& out) const {
  out << this->h << endl;
  out << this->a << endl;
  out << this->b << endl;
}

void libutt::SplitProof::read(std::istream& in) {
  in >> this->h;
  libff::consume_OUTPUT_NEWLINE(in);

  in >> this->a;
  libff::consume_OUTPUT_NEWLINE(in);

  in >> this->b;
  libff::consume_OUTPUT_NEWLINE(in);
}

std::ostream& operator<<(std::ostream& out, const libutt::SplitProof& sp) {
  sp.write(out);
  return out;
}

std::istream& operator>>(std::istream& in, libutt::SplitProof& sp) {
  sp.read(in);
  return in;
}

libutt::SplitProof::SplitProof(std::istream& in) { this->read(in); }

// Serialization/Deserialization for TxIn
void libutt::TxIn::write(std::ostream& out) const {
  out << this->null << endl;
  out << this->cm_val << endl;
  out << this->cc << endl;
  out << this->sig << endl;
  out << this->pi << endl;
}

void libutt::TxIn::read(std::istream& in) {
  in >> this->null;
  libff::consume_OUTPUT_NEWLINE(in);

  in >> this->cm_val;
  libff::consume_OUTPUT_NEWLINE(in);

  in >> this->cc;
  libff::consume_OUTPUT_NEWLINE(in);

  in >> this->sig;
  libff::consume_OUTPUT_NEWLINE(in);

  in >> this->pi;
  libff::consume_OUTPUT_NEWLINE(in);
}

std::ostream& operator<<(std::ostream& out, const libutt::TxIn& tx_in) {
  tx_in.write(out);
  return out;
}

std::istream& operator>>(std::istream& in, libutt::TxIn& tx_in) {
  tx_in.read(in);
  return in;
}

libutt::TxIn::TxIn(std::istream& in) : TxIn() { this->read(in); }

// Serialization/Deserialization for TxOut
void libutt::TxOut::write(std::ostream& out) const {
  out << this->epk << endl;
  out << this->ctxt << endl;
  out << this->cc_val << endl;
  out << this->range_pi << endl;

  out << this->epk_dh << endl;

  bool is_null = this->cc == nullptr;
  out << is_null << endl;
  if (!is_null) {
    out << *this->cc << endl;
  }
  is_null = this->sig == nullptr;
  out << is_null << endl;
  if (!is_null) {
    out << *this->sig << endl;
  }
}

void libutt::TxOut::read(std::istream& in) {
  in >> this->epk;
  libff::consume_OUTPUT_NEWLINE(in);

  in >> this->ctxt;
  libff::consume_OUTPUT_NEWLINE(in);

  in >> this->cc_val;
  libff::consume_OUTPUT_NEWLINE(in);

  in >> this->range_pi;
  libff::consume_OUTPUT_NEWLINE(in);

  in >> this->epk_dh;
  libff::consume_OUTPUT_NEWLINE(in);

  bool is_null;
  in >> is_null;
  libff::consume_OUTPUT_NEWLINE(in);

  if (!is_null) {
    libutt::CoinComm cc(in);
    this->cc = std::make_unique<libutt::CoinComm>(cc);
    libff::consume_OUTPUT_NEWLINE(in);
  } else {
    this->cc = nullptr;
  }

  in >> is_null;
  libff::consume_OUTPUT_NEWLINE(in);
  if (!is_null) {
    libutt::CoinSig sig(in);
    this->sig = std::make_unique<libutt::CoinSig>(sig);
    libff::consume_OUTPUT_NEWLINE(in);
  } else {
    this->sig = nullptr;
  }
}

std::ostream& operator<<(std::ostream& out, const libutt::TxOut& tx_out) {
  tx_out.write(out);
  return out;
}

std::istream& operator>>(std::istream& in, libutt::TxOut& tx_out) {
  tx_out.read(in);
  return in;
}

libutt::TxOut::TxOut(std::istream& in) : TxOut() { this->read(in); }

}  // namespace libutt
