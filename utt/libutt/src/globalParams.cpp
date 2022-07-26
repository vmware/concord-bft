#include <sstream>

#include "globalParams.hpp"
#include <utt/RangeProof.h>
#include <utt/NtlLib.h>
#include <utt/Params.h>

namespace libutt::api {
using Fr = typename libff::default_ec_pp::Fp_type;
void GlobalParams::init() {
  if (params != nullptr) return;
  unsigned char* randSeed = nullptr;  // TODO: initialize entropy source
  int size = 0;                       // TODO: initialize entropy source

  // Apparently, libff logs some extra info when computing pairings
  libff::inhibit_profiling_info = true;

  // AB: We _info disables printing of information and _counters prevents tracking of profiling information. If we are
  // using the code in parallel, disable both the logs.
  libff::inhibit_profiling_counters = true;

  // Initializes the default EC curve, so as to avoid "surprises"
  libff::default_ec_pp::init_public_params();

  // Initializes the NTL finite field
  NTL::ZZ p = NTL::conv<ZZ>("21888242871839275222246405745257275088548364400416034343698204186575808495617");
  NTL::ZZ_p::init(p);

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
  params.reset(new libutt::Params());
}
const libutt::Params& GlobalParams::getParams() const { return *params; }
libutt::Params& GlobalParams::getParams() { return *params; }
}  // namespace libutt::api