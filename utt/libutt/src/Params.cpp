#include <utt/Configuration.h>

#include <iostream>

#include <utt/Params.h>

#include <xutils/Log.h>

// WARNING: Define (de)serialization stream operators in the *global* namespace
std::ostream& operator<<(std::ostream& out, const libutt::Params& p) {
  out << p.ck_coin;
  out << p.ck_reg;
  out << p.ck_val;
  out << p.null;
  out << p.ibe;
  out << p.rpp;
  return out;
}

// WARNING: Define (de)serialization stream operators in the *global* namespace
std::istream& operator>>(std::istream& in, libutt::Params& p) {
  in >> p.ck_coin;
  in >> p.ck_reg;
  in >> p.ck_val;
  in >> p.null;
  in >> p.ibe;
  in >> p.rpp;
  return in;
}

namespace libutt {

Params Params::random() {
  /**
   * Pick the coin commitment key (and thus the PS16 g and \tilde{g} bases)
   */
  return Params::random(CommKey::random(Params::NumMessages));
}

Params Params::random(const CommKey& ck_coin) {
  Params p;

  p.ck_coin = ck_coin;

  // We need these for the setting up the registration CK and the value CK
  G1 g_1 = p.ck_coin.g[0];
  G1 g_3 = p.ck_coin.g[2];
  G1 g = p.ck_coin.getGen1();

  G2 g_tilde_1 = p.ck_coin.g_tilde[0];
  G2 g_tilde_3 = p.ck_coin.g_tilde[2];
  G2 g_tilde = p.ck_coin.getGen2();

  /**
   * The registration commitment CK reuses the same g_1 base for the 'pid'
   * as in the coin commitment CK (to make the splitproof easier, I hope)
   * but uses different base for the PRF key s.
   */
  Fr e = Fr::random_element();

  auto& rg = p.ck_reg.g;  // set up g vector for reg CK
  rg.push_back(g_1);      // g_1 from ck_coin
  rg.push_back(e * g);    // fresh g_{Params::NumMessages + 1}
  rg.push_back(g);        // g from ck_coin

  // proceed analogously for g_tilde vector for reg CK
  auto& rg_tilde = p.ck_reg.g_tilde;
  rg_tilde.push_back(g_tilde_1);
  rg_tilde.push_back(e * g_tilde);
  rg_tilde.push_back(g_tilde);

  /**
   * The value commitment CK is (g_3, g)
   */
  p.ck_val.g.push_back(g_3);  // g_3 from ck_coin
  p.ck_val.g.push_back(g);    // g from ck_coin

  p.ck_val.g_tilde.push_back(g_tilde_3);
  p.ck_val.g_tilde.push_back(g_tilde);

  /**
   * PRF public parameters
   *
   * PRF output is:
   *
   *    n = PRF_s(sn) = h^{1/(s+sn)}
   *
   * Randomized verification key is:
   *
   *     VK = h_tilde^s w_tilde^r, where r is random
   *
   * Auxiliary info to verify PRF against VK is:
   *
   *     y = e(n, w_tilde^r)
   */
  p.null = Nullifier::Params::random();

  p.ibe = IBE::Params::random();

  p.rpp = RangeProof::Params::random(p.getValCK());

  return p;
}

bool Params::operator==(const Params& o) const {
  return ck_coin == o.ck_coin && ck_reg == o.ck_reg &&
         //
         null == o.null &&
         //
         ibe == o.ibe && rpp == o.rpp && true;
}
}  // namespace libutt
