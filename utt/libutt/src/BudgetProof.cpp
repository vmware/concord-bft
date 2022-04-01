#include <utt/Configuration.h>

#include <utt/BudgetProof.h>

#include <utt/Serialization.h>  // WARNING: must be included last

using libff::operator<<;
using libff::operator>>;

std::ostream& operator<<(std::ostream& out, const libutt::BudgetProof& pi) {
  out << pi.forMeTxos << endl;
  out << pi.alpha << endl;
  out << pi.beta << endl;
  out << pi.e << endl;
  return out;
}

std::istream& operator>>(std::istream& in, libutt::BudgetProof& pi) {
  in >> pi.forMeTxos;
  libff::consume_OUTPUT_NEWLINE(in);

  in >> pi.alpha;
  libff::consume_OUTPUT_NEWLINE(in);

  in >> pi.beta;
  libff::consume_OUTPUT_NEWLINE(in);

  in >> pi.e;
  libff::consume_OUTPUT_NEWLINE(in);

  return in;
}

namespace libutt {

Fr BudgetProof::hash(const CommKey& regCK,
                     const Comm& rcm,
                     const std::vector<CommKey>& cks,
                     const std::vector<Comm>& icms,
                     const G1& X,
                     const std::vector<G1>& Y) const {
  std::stringstream ss;

  ss << regCK << endl << rcm << endl << X << endl << Y << endl;

  serializeVector(ss, cks);
  serializeVector(ss, icms);

  return hashToField(ss.str());
}

}  // namespace libutt
