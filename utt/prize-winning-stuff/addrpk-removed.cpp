

// method of AddrSK
AddrPK toAddrPK(const Params& p) const;

/**
 * Each user has an address key pair, consisting of an address public key and an address secret key.
 *
 * Alice needs Bob's AddrPK when paying him.
 */
class AddrPK {
 public:
  G1 pk1;  // i.e., g_1^s
  G2 pk2;  // i.e., \tilde{g}_1^s; we need this for the G2 counterpart of the coin commitment

  // Re-randomizable signature on g_1^s a.k.a., a zerosig on the zerocoin g_1^s g_2^0 g_3^0 g^0
  // TODO: implement
  std::optional<RandSig> zsig;

 public:
  // encryption public key
  G1 enc;

 public:
  AddrPK(const Params& p, const AddrSK& ask);

 public:
  /**
   * Derives a coin of denomination zero from the user's AddrPK with the specified randomness.
   *
   * This is used when creating the TxOut's in a transaction.
   */
  Comm toZerocoin(const Params& p, const Fr& randomizer) const;

  bool hasZerosig() const { return zsig.has_value(); }
};

AddrPK::AddrPK(const Params& p, const AddrSK& ask)
    : pk1(ask.s * p.pedAddrSkBase1()), pk2(ask.s * p.pedAddrSkBase2()), enc(ask.e * p.encBase()) {}

Comm AddrPK::toZerocoin(const Params& p, const Fr& randomizer) const {
  Comm cc;

  cc.ped1 = randomizer * p.ck.getGen1() + pk1;
  cc.ped2 = randomizer * p.ck.getGen2() + pk2;

  return cc;
}

/**
 * Address SK
 */
AddrPK AddrSK::toAddrPK(const Params& p) const { return AddrPK(p, *this); }
