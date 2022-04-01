#pragma once

#include <libff/algebra/curves/public_params.hpp>
#include <libff/common/default_types/ec_pp.hpp>
#include <libff/algebra/scalar_multiplication/multiexp.hpp>

#include <utt/internal/PicoSha2.h>

#include <xassert/XAssert.h>
#include <xutils/AutoBuf.h>

using std::endl;

namespace libutt {

// Type of group G1
using G1 = typename libff::default_ec_pp::G1_type;
// Type of group G2
using G2 = typename libff::default_ec_pp::G2_type;
// Type of group GT (recall pairing e : G1 x G2 -> GT)
using GT = typename libff::default_ec_pp::GT_type;
// Type of the finite field "in the exponent" of the EC group elements
using Fr = typename libff::default_ec_pp::Fp_type;

constexpr size_t _g1_size = 32;
constexpr size_t _g2_size = 64;
constexpr size_t _gt_size = 384;
constexpr size_t _fr_size = 128;

// Pairing function, takes an element in G1, another in G2 and returns the one in GT
// using libff::default_ec_pp::reduced_pairing;
// using ECPP::reduced_pairing;
// Found this solution on SO: https://stackoverflow.com/questions/9864125/c11-how-to-alias-a-function
// ('using ECPP::reduced_pairing;' doesn't work, even if you expand ECPP)
template <typename... Args>
auto ReducedPairing(Args&&... args) -> decltype(libff::default_ec_pp::reduced_pairing(std::forward<Args>(args)...)) {
  return libff::default_ec_pp::reduced_pairing(std::forward<Args>(args)...);
}

/**
 * Initializes the library, including its randomness.
 */
void initialize(unsigned char* randSeed, int size = 0);

/**
 * Faster than computing pairings sequentially.
 */
GT MultiPairing(const std::vector<G1>& g1, const std::vector<G2>& g2);
GT MultiPairingNaive(const std::vector<G1>& g1, const std::vector<G2>& g2);

/**
 * Hashes the specified string/bytes to a field element.
 */
Fr hashToField(const unsigned char* bytes, size_t len);
Fr hashToField(const std::string& message);

template <class Group>
Group hashToGroup(const std::string& message) {
  // TODO(Crypto:Hashing): Completely insecure, but we need something.
  // This is because libff does not implement hashing to curves.
  Group h = hashToField(message) * Group::one();
  return h;
}

/**
 * Hashes the specified group elements to a hexadecimal string.
 * WARNING: Likely insecure because of non-canonical formatting.
 */
template <class Group>
std::string __groupElemsAsString(const std::vector<Group>& elems) {
  // convert the group elements to a canonical string
  std::stringstream ss;
  if (elems.size() == 0) throw std::runtime_error("will not hash empty vector of group elements");

  ss << elems[0] << endl;
  if (elems.size() > 0) {
    for (size_t i = 0; i < elems.size() - 1; i++) {
      ss << "|" << elems[i];
    }
  }

  return ss.str();
}

template <class Group>
std::string hashToHex(const std::vector<Group>& elems) {
  std::string str = __groupElemsAsString(elems), hex;
  auto indata = str.c_str();

  picosha2::hash256_hex_string(indata, indata + str.size(), hex);
  return hex;
}

template <class Group>
std::string hashToHex(const Group& elem) {
  std::vector<Group> v = {elem};
  return hashToHex<Group>(v);
}

template <class Group>
void hashToBytes(
    const Group& elem, const std::string& prefix, const std::string& suffix, unsigned char* hash, size_t capacity) {
  std::stringstream ss;

  ss << prefix;
  ss << elem;
  ss << suffix;

  std::string str = ss.str();
  if (capacity < picosha2::k_digest_size) throw std::runtime_error("Need a larger hash buffer");

  picosha2::hash256(str, hash, hash + picosha2::k_digest_size);
}

template <class Group>
AutoBuf<unsigned char> hashToBytes(const Group& elem, const std::string& prefix = "", const std::string& suffix = "") {
  AutoBuf<unsigned char> hash(picosha2::k_digest_size);

  hashToBytes(elem, prefix, suffix, hash.getBuf(), hash.size());

  return hash;
}

/**
 * Hashes the specified group elements to a field element.
 * WARNING: Likely insecure because of non-canonical formatting.
 */
template <class Group>
Fr hashToField(const std::vector<Group>& elems) {
  std::string str = __groupElemsAsString(elems);

  return hashToField(reinterpret_cast<const unsigned char*>(str.c_str()), str.size());
}

template <class Group>
Fr hashToField(const Group& elem) {
  std::vector<Group> v = {elem};
  return hashToField<Group>(v);
}

/**
 * WARNING: Not safe for sending across the network to different machines.
 */
size_t Fr_num_bytes();  // size of a serialized Fr
void Fr_serialize(const Fr& fr, unsigned char* bytes, size_t capacity);
Fr Fr_deserialize(const unsigned char* bytes, size_t len);

AutoBuf<unsigned char> frsToBytes(const std::vector<Fr>& frs);

std::vector<Fr> bytesToFrs(const AutoBuf<unsigned char>& buf);

/**
 * Returns some random bytes via the C++ RNG.
 * WARNING: Insecure.
 */
void random_bytes(unsigned char* bytes, size_t len);

/**
 * Picks num random field elements
 */
std::vector<Fr> random_field_elems(size_t num);

template <class Group>
std::vector<Group> random_group_elems(size_t n) {
  std::vector<Group> bases;
  for (size_t j = 0; j < n; j++) {
    bases.push_back(Group::random_element());
  }
  return bases;
}

size_t getNumCores();

/**
 * Returns the first n Nth roots of unity where N = 2^k is the smallest number such that n <= N.
 */
std::vector<Fr> get_all_roots_of_unity(size_t n);

/**
 * Picks a random polynomial of degree deg.
 */
// std::vector<Fr> random_poly(size_t degree);

/**
 * Performs a multi-exponentiation using libfqfft
 */
template <class Group>
Group multiExp(typename std::vector<Group>::const_iterator base_begin,
               typename std::vector<Group>::const_iterator base_end,
               typename std::vector<Fr>::const_iterator exp_begin,
               typename std::vector<Fr>::const_iterator exp_end) {
  long sz = base_end - base_begin;
  long expsz = exp_end - exp_begin;
  if (sz != expsz) throw std::runtime_error("multiExp needs the same number of bases as exponents");
  // size_t numCores = getNumCores();

  if (sz > 4) {
    if (sz > 16384) {
      return libff::multi_exp<Group, Fr, libff::multi_exp_method_BDLO12>(
          base_begin, base_end, exp_begin, exp_end, 1);  // numCores);
    } else {
      return libff::multi_exp<Group, Fr, libff::multi_exp_method_bos_coster>(
          base_begin, base_end, exp_begin, exp_end, 1);  // numCores);
    }
  } else {
    return libff::multi_exp<Group, Fr, libff::multi_exp_method_naive>(base_begin, base_end, exp_begin, exp_end, 1);
  }
}

/**
 * Performs a multi-exponentiation using libfqfft
 */
template <class Group>
Group multiExp(const std::vector<Group>& bases, const std::vector<Fr>& exps) {
  assertEqual(bases.size(), exps.size());
  return multiExp<Group>(bases.cbegin(), bases.cend(), exps.cbegin(), exps.cend());
}

// We often use multiexps of size 1 or 2
template <class Group>
Group multiExp(const Group& b1, const Group& b2, const Group& b3, const Fr& e1, const Fr& e2, const Fr& e3) {
  std::vector<Group> b = {b1, b2};
  std::vector<Fr> exps = {e1, e2};

  // b.c. we call this with b3 (or e3) set to the identity when we want to do a multiexp of size 2
  if (e3 != Fr::zero() && b3 != Group::zero()) {
    b.push_back(b3);
    exps.push_back(e3);
  }

  return multiExp(b, exps);
}

template <class Group>
Group multiExp(const Group& b1, const Group& b2, const Fr& e1, const Fr& e2) {
  return multiExp(b1, b2, Group::zero(), e1, e2, Fr::zero());
}

}  // end of namespace libutt

namespace boost {

std::size_t hash_value(const libutt::Fr& f);

}  // namespace boost
