#include <utt/Configuration.h>

#include <fstream>
#include <istream>
#include <ostream>
#if __has_include(<filesystem>)
#include <filesystem>
namespace fs = std::filesystem;
#elif __has_include(<experimental/filesystem>)
#include <experimental/filesystem>
namespace fs = std::experimental::filesystem;
#else
error "Missing the <filesystem> header."
#endif

#include <utt/PolyOps.h>
#include <utt/RandSigDKG.h>

#include <xutils/Log.h>
#include <xutils/Utils.h>

namespace libutt {

RandSigDKG::RandSigDKG(size_t t, size_t n, size_t ell) {
  // degree t-1 polynomial f(X) such that t players can reconstruct f(0) = x, which encodes the bank's secret key x
  std::vector<Fr> poly_f = random_field_elems(t);
  Fr x = poly_f[0];

  // degree t-1 polynomials Y_k(X) for secret sharing the y_k = Y_k(0)
  std::vector<std::vector<Fr>> polys_y;
  // PS16 secrets y_1, y_2, \dots, y_k
  std::vector<Fr> y;
  for (size_t k = 0; k < ell; k++) {
    auto poly = random_field_elems(t);
    polys_y.push_back(poly);
    y.push_back(poly[0]);
  }

  // generate the commitment key related to the PS16 secrets above
  G1 g = G1::random_element();
  G2 g_tilde = G2::random_element();
  ck = CommKey::fromTrapdoor(g, g_tilde, y);

  // initialize the single-server PS16 SK (useful for testing)
  sk.ck = ck;
  sk.x = x;
  sk.y = y;
  sk.X = x * g;

  /**
   * Compute the SK shares [x]_i, [y_k]_i, \forall k\in \{1,\dots,\ell\}
   * i.e., evaluate the secret-sharing polys at n pre-determined points (i.e., Nth roots of unity, with smallest N = 2^k
   * >= n)
   */
  std::vector<Fr> evals_f = poly_fft(poly_f, n);

  std::vector<std::vector<Fr>> evals_y;
  for (size_t k = 0; k < ell; k++) {
    evals_y.push_back(poly_fft(polys_y[k], n));
  }

  // [x]_i = f(\omega_N^i)
  // [y_k]_i = Y_k(\omega_N^i), \forall k\in\{1,\dots,\ell\}
  for (size_t i = 0; i < n; i++) {
    RandSigShareSK ss;

    ss.ck = ck;
    ss.x_and_ys.push_back(evals_f[i]);
    for (size_t k = 0; k < ell; k++) {
      ss.x_and_ys.push_back(evals_y[k][i]);
    }

    skShares.push_back(ss);
  }
}

void RandSigDKG::writeToFiles(const std::string& dirPath, const std::string& fileNamePrefix) const {
  for (size_t i = 0; i < skShares.size(); i++) {
    std::string filePath = dirPath;
    filePath += "/";
    filePath += fileNamePrefix + std::to_string(i);

    loginfo << "Writing params and SK for replica #" << i << " to " << filePath << endl;

    // make sure the file does not already exist, since we do not want to accidentally overwrite other secrets
    fs::path fsp(filePath);
    if (fs::exists(fsp)) {
      logerror << "Output file " << filePath << " should not exist." << endl;
      throw std::runtime_error("File already exists");
    }

    std::ofstream fout(filePath);

    fout << ck;
    fout << skShares[i];

    // useful for the BFT setting to write the aggregate PK too
    fout << getPK();
  }
}

RandSigPK RandSigDKG::aggregatePK(size_t n, std::vector<RandSigSharePK>& pkShares, std::vector<size_t>& ids) {
  testAssertStrictlyGreaterThan(pkShares.size(), 0);

  auto lagr = lagrange_coefficients_naive(n, ids);

  std::vector<G2> g2Shares;
  g2Shares.reserve(pkShares.size());

  G1 g = pkShares[0].g;
  G2 g_tilde = pkShares[0].g_tilde;
  size_t ell = pkShares[0].Y_tilde.size();
  for (auto& pk : pkShares) {
    g2Shares.push_back(pk.X_tilde);

    // NOTE: Just being extra cautious and making sure all share PKs have the same g_tilde and # of y_k's
    testAssertEqual(pk.g, g);
    testAssertEqual(pk.g_tilde, g_tilde);
    testAssertEqual(pk.Y_tilde.size(), ell);
  }

  logdbg << "lagr.size(): " << lagr.size() << endl;
  logdbg << "g2Shares.size(): " << g2Shares.size() << endl;

  RandSigPK pk;
  // TODO: this is not sane; we need to stop constructing objects ad-hoc like this since, when
  // we update an object by (say) adding a field, we have to update all the places where we construct it too
  // and of course, we will forget one of those places
  pk.g = g;
  pk.g_tilde = g_tilde;
  pk.X_tilde = multiExp(g2Shares, lagr);

  pk.Y_tilde.resize(ell);
  for (size_t k = 0; k < ell; k++) {
    g2Shares.resize(0);
    for (auto& pk : pkShares) {
      g2Shares.push_back(pk.Y_tilde[k]);
    }

    pk.Y_tilde[k] = multiExp(g2Shares, lagr);
  }

  return pk;
}
}  // namespace libutt
