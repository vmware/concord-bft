#include <sstream>

#include "UTTParams.hpp"
#include <utt/RangeProof.h>
#include <utt/NtlLib.h>
#include <utt/Params.h>

namespace libutt::api {
struct UTTParams::Impl {
  Impl(libutt::Params params_) : params{params_} {}
  Impl() = default;
  libutt::Params params;
};

using Fr = typename libff::default_ec_pp::Fp_type;
struct GpInitData {
  libutt::CommKey cck;
  libutt::CommKey rck;
};
void UTTParams::initLibs(const UTTParams::BaseLibsInitData& init_data) {
  // Apparently, libff logs some extra info when computing pairings
  libff::inhibit_profiling_info = init_data.libff_inhibit_profiling_info;

  // AB: We _info disables printing of information and _counters prevents tracking of profiling information. If we are
  // using the code in parallel, disable both the logs.
  libff::inhibit_profiling_counters = init_data.libff_inhibit_profiling_counters;

  // Initializes the default EC curve, so as to avoid "surprises"
  libff::default_ec_pp::init_public_params();

  // Initializes the NTL finite field
  NTL::ZZ p = NTL::conv<ZZ>("21888242871839275222246405745257275088548364400416034343698204186575808495617");
  NTL::ZZ_p::init(p);

  NTL::SetSeed(init_data.entropy_source.first, init_data.entropy_source.second);

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

UTTParams UTTParams::create(void* initData) {
  UTTParams gp;
  gp.impl_.reset(new UTTParams::Impl());
  if (initData) {
    GpInitData* init_data = (GpInitData*)initData;
    gp.impl_->params = libutt::Params::random(init_data->cck);
    gp.impl_->params.ck_reg = init_data->rck;
  }
  return gp;
}
void* UTTParams::getParams() const { return &(impl_->params); }

UTTParams::UTTParams(const UTTParams& other) {
  impl_.reset(new UTTParams::Impl());
  impl_->params = other.impl_->params;
}
UTTParams& UTTParams::operator=(const UTTParams& other) {
  if (this == &other) return *this;
  impl_.reset(new UTTParams::Impl());
  impl_->params = other.impl_->params;
  return *this;
}
}  // namespace libutt::api

std::ostream& operator<<(std::ostream& out, const libutt::api::UTTParams& params) {
  out << params.impl_->params;
  return out;
}
std::istream& operator>>(std::istream& in, libutt::api::UTTParams& params) {
  if (params.impl_ == nullptr) params.impl_.reset(new libutt::api::UTTParams::Impl());
  in >> params.impl_->params;
  return in;
}
bool operator==(const libutt::api::UTTParams& params1, const libutt::api::UTTParams& params2) {
  return params1.impl_->params == params2.impl_->params;
}