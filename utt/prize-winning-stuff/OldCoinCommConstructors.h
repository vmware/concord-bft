
// CoinComm(const Params& p, const long val, const Fr& r, bool withG2 = true)
//    : CoinComm(p, Fr(val), r, withG2)
//{}

// Use this constructor to create commitments to a value with esk=0 (During minting/payment)
// CoinComm(const Params& p, const Fr& val, const Fr& r, bool withG2 = true)
//{
//    // FIXME: For some reason, I can no longer use the STL iterators over G1/Fr vectors, I think due to some missing
//    default constructors in libff which C++17 borks on.
//    // This is why the code below (as well as other code) is awkwardly setting up subvectors.
//    ped1 = multiExp(
//        p.pedDenomBase1(), p.pedRandBase1(),
//        val, r);

//    if(withG2) {
//        ped2 = multiExp(
//            p.pedDenomBase2(), p.pedRandBase2(),
//            val, r);
//    } else {
//        ped2 = G2::zero();
//    }
//}

/**
 * Builds a coin commitment from an EPK, a coin value and a commitment randomizer
 */
// CoinComm(const Params& p, const EPK& epk, const Fr& val, const Fr& r, bool withG2 = true)
//    : CoinComm(p, val, r, withG2)
//{
//    ped1 = ped1 + epk.asG1();

//    if(withG2) {
//        ped2 = ped2 + epk.asG2();
//    }
//}

// Build a commitment of the form g_1^esk * g_2^v * g^r with r=0
// This is used in the BFT setting by the replicas during Minting
// CoinComm(const Params& p, const EPK& epk, long val, bool withG2 = true)
//    : CoinComm(p, epk, Fr(val), Fr::zero(), withG2)
//{}

/**
 * When processing a TXN after it is verified, we need to compute
 * the output coin commitment to be signed by combining the EPK and
 * value commitments.
 */
// CoinComm(const EPK& epk, const CoinComm& valComm)
//    : ped1(epk.asG1() + valComm.ped1), ped2(epk.asG2() + valComm.asG2())
//{
//}
