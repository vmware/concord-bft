#include <utt/Configuration.h>

#include <algorithm>
#include <climits>
#include <fstream>
#include <functional>
#include <random>
#include <thread>
#include <vector>

#include <boost/functional/hash.hpp>

#ifdef USE_MULTITHREADING
#include <omp.h>
#endif

#include <utt/internal/PicoSha2.h>
#include <utt/NtlLib.h>
#include <utt/PolyCrypto.h>
#include <utt/RangeProof.h>

#include <libff/common/profiling.hpp>
#include <libff/algebra/field_utils/field_utils.hpp>  // get_root_of_unity

#include <xutils/Utils.h>

using std::endl;
using namespace NTL;

namespace libutt {

using random_bytes_engine = std::independent_bits_engine<std::default_random_engine, CHAR_BIT, unsigned char>;

void initialize(unsigned char* randSeed, int size) {
  (void)randSeed;  // TODO: initialize entropy source
  (void)size;      // TODO: initialize entropy source

  // Apparently, libff logs some extra info when computing pairings
  libff::inhibit_profiling_info = true;

  // AB: We _info disables printing of information and _counters prevents tracking of profiling information. If we are
  // using the code in parallel, disable both the logs.
  libff::inhibit_profiling_counters = true;

  // Initializes the default EC curve, so as to avoid "surprises"
  libff::default_ec_pp::init_public_params();

  // Initializes the NTL finite field
  ZZ p = conv<ZZ>("21888242871839275222246405745257275088548364400416034343698204186575808495617");
  ZZ_p::init(p);

  NTL::SetSeed(randSeed, size);

#ifdef USE_MULTITHREADING
  // NOTE: See https://stackoverflow.com/questions/11095309/openmp-set-num-threads-is-not-working
  loginfo << "Using " << getNumCores() << " threads" << endl;
  omp_set_dynamic(0);                                    // Explicitly disable dynamic teams
  omp_set_num_threads(static_cast<int>(getNumCores()));  // Use 4 threads for all consecutive parallel regions
#else
  // loginfo << "NOT using multithreading" << endl;
#endif

  RangeProof::Params::initializeOmegas();
}

GT MultiPairingNaive(const std::vector<G1>& g1, const std::vector<G2>& g2) {
  assertEqual(g1.size(), g2.size());
  GT r = GT::one();
  for (size_t i = 0; i < g1.size(); i++) {
    r = r * ReducedPairing(g1[i], g2[i]);
  }
  return r;
}

GT MultiPairing(const std::vector<G1>& g1, const std::vector<G2>& g2) {
  assertEqual(g1.size(), g2.size());
  using G1_precomp = libff::default_ec_pp::G1_precomp_type;
  using G2_precomp = libff::default_ec_pp::G2_precomp_type;
  using Fqk = libff::default_ec_pp::Fqk_type;

  std::vector<G1_precomp> g1p;
  std::vector<G2_precomp> g2p;

  for (auto el : g1) {
    g1p.push_back(libff::default_ec_pp::precompute_G1(el));
  }

  for (auto el : g2) {
    g2p.push_back(libff::default_ec_pp::precompute_G2(el));
  }

  auto numDblMiller = g1.size() / 2;
  bool singleMiller = (g1.size() % 2 == 1);
  Fqk r = Fqk::one();
  for (size_t i = 0; i < numDblMiller; i++) {
    r = r * libff::default_ec_pp::double_miller_loop(g1p[2 * i], g2p[2 * i], g1p[2 * i + 1], g2p[2 * i + 1]);
  }

  if (singleMiller) {
    r = r * libff::default_ec_pp::miller_loop(g1p.back(), g2p.back());
  }

  return libff::default_ec_pp::final_exponentiation(r);
}

Fr hashToField(const unsigned char* bytes, size_t len) {
  // hash bytes, but output a hex string not bytes
  std::string hex;
  picosha2::hash256_hex_string(bytes, bytes + len, hex);
  hex.pop_back();  // small enough for BN-P254 field

  // convert hex to mpz_t
  mpz_t rop;
  mpz_init(rop);
  mpz_set_str(rop, hex.c_str(), 16);

  // convert mpz_t to Fr
  Fr fr = libff::bigint<Fr::num_limbs>(rop);
  mpz_clear(rop);

  return fr;
}

Fr hashToField(const std::string& message) {
  return hashToField(reinterpret_cast<const unsigned char*>(message.c_str()), message.size());
}

size_t Fr_num_bytes() {
  const size_t assumedFieldSize = 32;  // bytes

#ifndef NDEBUG
  // make sure Fr's are actually 32 bytes
  Fr test = Fr::random_element();
  std::vector<uint64_t> v = test.to_words();
  // a byte is 8 bits => 64 bits is 8 bytes => need 4 words in 'v'
  assertEqual(v.size(), 4);
#endif

  return assumedFieldSize;
}

void Fr_serialize(const Fr& fr, unsigned char* bytes, size_t capacity) {
  std::vector<uint64_t> words = fr.to_words();

  size_t wordSize = sizeof(uint64_t);

  if (capacity < wordSize * words.size()) {
    throw std::runtime_error("Need larger buffer to serialize Fr");
  }

  for (auto& w : words) {
    std::memcpy(bytes, reinterpret_cast<unsigned char*>(&w), wordSize);
    bytes += wordSize;
  }
}

Fr Fr_deserialize(const unsigned char* bytes, size_t len) {
  Fr a;
  size_t wordSize = sizeof(uint64_t);
  size_t numWords = len / wordSize;
  std::vector<uint64_t> words(numWords);
  assertEqual(words.size(), numWords);

  for (size_t i = 0; i < words.size(); i++) {
    uint64_t w;
    std::memcpy(reinterpret_cast<unsigned char*>(&w), bytes, wordSize);
    bytes += wordSize;
    words[i] = w;
  }

  a.from_words(words);

  return a;
}

AutoBuf<unsigned char> frsToBytes(const std::vector<Fr>& frs) {
  size_t frSize = Fr_num_bytes();

  AutoBuf<unsigned char> buf(frSize * frs.size());

  for (size_t i = 0; i < frs.size(); i++) Fr_serialize(frs[i], buf.getBuf() + i * frSize, frSize);

  return buf;
}

std::vector<Fr> bytesToFrs(const AutoBuf<unsigned char>& buf) {
  std::vector<Fr> frs;

  size_t frSize = Fr_num_bytes();
  size_t num = buf.size() / frSize;

  for (size_t i = 0; i < num; i++) {
    auto val = Fr_deserialize(buf.getBuf() + i * frSize, frSize);

    frs.push_back(val);
  }

  return frs;
}

void random_bytes(unsigned char* bytes, size_t len) {
  random_bytes_engine rbe;
  std::generate(bytes, bytes + len, std::ref(rbe));
}

std::vector<Fr> random_field_elems(size_t num) {
  std::vector<Fr> p(num);
  for (size_t i = 0; i < p.size(); i++) {
    p[i] = Fr::random_element();
  }
  return p;
}

size_t getNumCores() {
  static size_t numCores = std::thread::hardware_concurrency();
  if (numCores == 0) throw std::runtime_error("Could not get number of cores");
  return numCores;
}

std::vector<Fr> get_all_roots_of_unity(size_t n) {
  if (n < 1) throw std::runtime_error("Cannot get 0th root-of-unity");

  size_t N = Utils::smallestPowerOfTwoAbove(n);

  // initialize array of roots of unity
  Fr omega = libff::get_root_of_unity<Fr>(N);
  std::vector<Fr> omegas(n);
  omegas[0] = Fr::one();

  if (n > 1) {
    omegas[1] = omega;
    for (size_t i = 2; i < n; i++) {
      omegas[i] = omega * omegas[i - 1];
    }
  }

  return omegas;
}

// std::vector<Fr> random_poly(size_t deg) {
//    return random_field_elems(deg+1);
//}

}  // end of namespace libutt

namespace boost {

std::size_t hash_value(const libutt::Fr& f) {
  size_t size;
  mpz_t rop;
  mpz_init(rop);
  f.as_bigint().to_mpz(rop);

  // char *s = mpz_get_str(NULL, 10, rop);
  // size = strlen(s);
  // auto h = boost::hash_range<char*>(s, s + size);

  // void (*freefunc)(void *, size_t);
  // mp_get_memory_functions(NULL, NULL, &freefunc);
  // freefunc(s, size);

  mpz_export(NULL, &size, 1, 1, 1, 0, rop);
  AutoBuf<unsigned char> buf(static_cast<long>(size));

  mpz_export(buf, &size, 1, 1, 1, 0, rop);
  auto h = boost::hash_range<unsigned char*>(buf, buf + buf.size());

  mpz_clear(rop);
  return h;
}

}  // namespace boost
