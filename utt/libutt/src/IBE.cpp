#include <utt/Configuration.h>

#include <functional>
#include <algorithm>

#include <utt/Address.h>
#include <utt/IBE.h>
#include <utt/Params.h>

#include <utt/internal/plusaes.h>

#include <xutils/AutoBuf.h>
#include <xutils/Log.h>
#include <xutils/Utils.h>

using namespace libutt;

std::ostream& operator<<(std::ostream& out, const libutt::IBE::Params& p) {
  out << p.g1;
  return out;
}

std::istream& operator>>(std::istream& in, libutt::IBE::Params& p) {
  in >> p.g1;
  return in;
}

std::ostream& operator<<(std::ostream& out, const libutt::IBE::MPK& mpk) {
  out << mpk.p;
  out << mpk.mpk;
  return out;
}

std::istream& operator>>(std::istream& in, libutt::IBE::MPK& mpk) {
  in >> mpk.p;
  in >> mpk.mpk;
  return in;
}

std::ostream& operator<<(std::ostream& out, const libutt::IBE::MSK& msk) {
  out << msk.msk;
  return out;
}
std::istream& operator>>(std::istream& in, libutt::IBE::MSK& msk) {
  in >> msk.msk;
  return in;
}
// WARNING: Serialization operators must be in the global namespace
std::ostream& operator<<(std::ostream& out, const libutt::IBE::Ctxt& c) {
  out << c.R << endl;
  out << Utils::bin2hex(c.buf1, c.buf1.size()) << endl;
  out << Utils::bin2hex(c.buf2, c.buf2.size()) << endl;

  return out;
}

// WARNING: Deserialization operator must be in the global namespace
std::istream& operator>>(std::istream& in, libutt::IBE::Ctxt& c) {
  in >> c.R;
  libff::consume_OUTPUT_NEWLINE(in);

  std::string buf1Hex, buf2Hex;
  in >> buf1Hex;
  libff::consume_OUTPUT_NEWLINE(in);

  in >> buf2Hex;
  libff::consume_OUTPUT_NEWLINE(in);

  c.buf1 = AutoBuf<unsigned char>(buf1Hex.size() / 2);
  Utils::hex2bin(buf1Hex, c.buf1.getBuf(), c.buf1.size());

  c.buf2 = AutoBuf<unsigned char>(buf2Hex.size() / 2);
  Utils::hex2bin(buf2Hex, c.buf2.getBuf(), c.buf2.size());

  return in;
}

std::ostream& operator<<(std::ostream& out, const libutt::IBE::EncSK& sk) {
  out << sk.pid << endl;
  out << sk.mpk;
  out << sk.encsk;
  return out;
}

std::istream& operator>>(std::istream& in, libutt::IBE::EncSK& sk) {
  in >> sk.pid;
  libff::consume_OUTPUT_NEWLINE(in);

  in >> sk.mpk;
  in >> sk.encsk;
  return in;
}

namespace libutt {

Fr IBE::Params::hashToR(const MPK& mpk,
                        const std::string& id,
                        const AutoBuf<unsigned char>& ptxt,
                        const AutoBuf<unsigned char>& sigma) {
  std::stringstream ss;
  std::string mpkStr;
  ss << mpk.mpk;
  ss >> mpkStr;

  std::string ptxtHex = Utils::bin2hex(ptxt.getBuf(), ptxt.size());
  std::string sigmaHex = Utils::bin2hex(sigma.getBuf(), sigma.size());

  return hashToField(std::string("ibe|hash-to-r") + "|" + mpkStr + "|" + id + "|" + ptxtHex + "|" + sigmaHex);
}

IBE::Ctxt::Ctxt(std::istream& in) : IBE::Ctxt() { in >> *this; }

IBE::Ctxt IBE::MPK::encrypt(const std::string& pid, const AutoBuf<unsigned char>& ptxt) const {
  // pick random \sigma
  AutoBuf<unsigned char> sigma(Params::RAND_NUM_BYTES);
  random_bytes(sigma.getBuf(), Params::RAND_NUM_BYTES);

  // pick random r via hash function
  Fr r = Params::hashToR(*this, pid, ptxt, sigma);

  // one time key entropy will be T = e(mpk, H_1(pid))^r = e(g_1, g_2)^{r h \msk}
  GT T = ReducedPairing(r * mpk, Params::hashID<G2>(pid));

  AutoBuf<unsigned char> otk1, otk2;
  std::tie(otk1, otk2) = Params::hashToOneTimeKeys(T, ptxt.size());

  assertEqual(otk1.size(), ptxt.size());
  assertEqual(otk2.size(), sigma.size());

  // first, store R = g_1^r in ciphertext
  Ctxt ctxt;
  ctxt.R = r * p.g1;

  // second, encrypt via XORs
  ctxt.buf1 = AutoBuf<unsigned char>(ptxt.size());
  std::transform(
      ptxt.getBuf(), ptxt.getBuf() + ptxt.size(), otk1.getBuf(), ctxt.buf1.getBuf(), std::bit_xor<unsigned char>());

  ctxt.buf2 = AutoBuf<unsigned char>(sigma.size());
  std::transform(
      sigma.getBuf(), sigma.getBuf() + sigma.size(), otk2.getBuf(), ctxt.buf2.getBuf(), std::bit_xor<unsigned char>());

  return ctxt;
}

std::tuple<bool, AutoBuf<unsigned char>> IBE::EncSK::decrypt(const IBE::Ctxt& ctxt) const {
  // parse g^r from ctxt
  G1 R = ctxt.R;  // denoted by 'u' in [Srin10], pg 80

  // one time key entropy will be T = e(g_1^r, H_1(pid)^\msk) = e(g_1, g_2)^{r h \msk}
  GT T = ReducedPairing(R, encsk);

  size_t ptxtSize = ctxt.buf1.size();
  AutoBuf<unsigned char> otk1, otk2;
  std::tie(otk1, otk2) = Params::hashToOneTimeKeys(T, ptxtSize);

  assertEqual(otk1.size(), ptxtSize);
  size_t sigmaSize = ctxt.buf2.size();
  assertEqual(otk2.size(), sigmaSize);

  // decrypt via XORs
  AutoBuf<unsigned char> ptxt(ptxtSize), sigma(sigmaSize);

  std::transform(
      ctxt.buf1.getBuf(), ctxt.buf1.getBuf() + ptxtSize, otk1.getBuf(), ptxt.getBuf(), std::bit_xor<unsigned char>());

  std::transform(
      ctxt.buf2.getBuf(), ctxt.buf2.getBuf() + sigmaSize, otk2.getBuf(), sigma.getBuf(), std::bit_xor<unsigned char>());

  bool success = (R == (Params::hashToR(mpk, pid, ptxt, sigma) * mpk.p.g1));

  return std::make_tuple(success, ptxt);
}

bool IBE::Ctxt::operator==(const IBE::Ctxt& o) const {
  if (R != o.R) {
    // logdbg << "Different R" << endl;
    return false;
  }

  if (buf1 != o.buf1) {
    // logdbg << "Different ciphertext buf" << endl;
    // logdbg << "  len: " << buf.size() << endl;
    // logdbg << "  o.len: " << o.buf.size() << endl;
    return false;
  }

  if (buf2 != o.buf2) {
    // logdbg << "Different ciphertext buf" << endl;
    // logdbg << "  len: " << buf.size() << endl;
    // logdbg << "  o.len: " << o.buf.size() << endl;
    return false;
  }

  // logdbg << "Same ciphertext!" << endl;
  return true;
}

}  // end of namespace libutt
