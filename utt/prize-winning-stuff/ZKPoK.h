    /**
     * We use this to keep track of which library version the benchmark numbers were for.
     * Actually, upon further thought, this should be much more flexible and be given as input
     * to the benchmarks.
     */
    //constexpr static const char * Version = "may24pres";

    /**
     * Proof for a revealed EPK being the committed EPK.
     */
    class SplitProof {
    public:
        // A Schnorr ZKSoK of esk w.r.t. epk = g_1^{esk}. This will also sign a message.
        Fr s_esk;
        Fr e_esk;

        // An Okamato ZKPoK of (v, r) w.r.t. g_2^v g^r = coin comm. / epk
        Fr s_v, s_r;
        Fr e_vr;

    public:
        SplitProof(const Params& p, const CoinSecrets& secrets, const std::string& msgToSign) {
            // Okamato ZKPoK for g_2^v g^r
            Fr k_v = Fr::random_element();
            Fr k_r = Fr::random_element();
            G1 R_vr = k_v * p.pedDenomBase1() + k_r * p.pedRandBase1();
            // TODO(Crypto): I don't think it's necessary to sign the message here; so I exclude it.
            e_vr = hashToField(R_vr);
            s_v = k_v - e_vr * secrets.val;
            s_r = k_r - e_vr * secrets.r;

            // Schnorr ZKPoK for epk = g_1^{esk}
            Fr k_esk = Fr::random_element();
            G1 R_esk = k_esk * p.pedEskBase1();
            e_esk = hashToField(msgToSign + "|" + hashToHex(R_esk));
            s_esk = k_esk - e_esk * secrets.esk.s;
        }

    public:
        bool verify(const Params& p, const EPK& epk, const CoinComm& comm, const std::string& msgSigned) const {
            //  get the commitment g_2^v g^r by dividing out g_1^esk
            G1 comm_vr = comm.ped1 - epk.asG1();

            // now check the ZKPoK for comm_vr = g_2^v g^r
            G1 g_2 = p.pedDenomBase1();
            G1 g   = p.pedRandBase1();
            std::vector<G1> bases = { g_2, g  , comm_vr };
            std::vector<Fr> exps =  { s_v, s_r, e_vr    };
            if (e_vr != hashToField(multiExp<G1>(bases, exps)))
                return false;
            
            // now check the ZKSoK for epk = g_1^esk
            G1 g_1 = p.pedEskBase1();
            if (e_esk != hashToField(msgSigned + "|" + hashToHex(s_esk * g_1 + e_esk * epk.asG1())))
                return false;

            return true;
        }
    };

