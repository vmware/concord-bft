#pragma once

#include <optional>
#include <tuple>

#include <utt/Coin.h>
#include <utt/Params.h>
#include <utt/PolyCrypto.h>
#include <utt/RandSig.h>
#include <utt/RangeProof.h>
#include <utt/ZKPoK.h>

#include <xassert/XAssert.h>
#include <xutils/AutoBuf.h>
#include <xutils/Log.h>
#include <xutils/NotImplementedException.h>

namespace libutt {
    class TxOut;
}

std::ostream& operator<<(std::ostream&, const libutt::TxOut&);
std::istream& operator>>(std::istream&, libutt::TxOut&); 

namespace libutt {

    class TxOut {
    public:
        Fr coin_type;
        Fr exp_date;

        mutable std::optional<G1> H;    // we do cache H here for the clients and server to re-use
        //CommKey ck_tx;          // 'icm' and 'vcm_2' are under CK (H, g), since that's what we need for threshold signing using PS16...

//      Fr z;                   // randomness for vcm_1 (is *never* serialized!)
        Comm vcm_1;             // commitment to coin value transferred to the recipient

        /**
         * Range proof for the coin value in 'vcm_1'
         *
         * For the budget output coin, do WE still need a range proof? After all,
         * the TXN proves that:
         *
         *   val_budget_out = val_budget_in - \sum_j val_recip(j)_out
         *
         * ...and everything on the RHS already has range proofs.
         *
         * Yes, we still need it, since we need a way to prove that:
         *
         *   val_budget_out >= 0,
         *
         * And right now the only thing we have is the range proof.
         */
        std::optional<RangeProof> range_pi;

        Fr d;                   // randomness for vcm_2 (is *never* serialized!)
        Comm vcm_2;             // re-commitment under ck_tx above
        PedEqProof vcm_eq_pi;   // proof that vcm_1 and vcm_2 commit to the same value
        
        Fr t;                   // randomness for 'icm' (is *never* serialized!)
        Comm icm;               // commitment to identity of the recipient, under ck_tx

        /**
         * ZKPoK of opening for icm
         *
         * NOTE: This icm_pok is unnecessary for outputs that go back to sender.
         * When TXNs are budgeted, the icm's in these outputs already go through the budget proof's PoK
         */
        std::optional<ZKPoK> icm_pok;

        IBE::Ctxt ctxt;         // encryption of coin value and coin commitment randomness

    public:
        size_t getSize() const {
            return 
                _fr_size + 
                _fr_size + 
                sizeof(bool) + (H.has_value() ? _g1_size: 0) +
                vcm_1.getSize() + 
                sizeof(bool) + (range_pi.has_value() ? range_pi->getSize(): 0) +
                _fr_size +
                vcm_2.getSize() + 
                vcm_eq_pi.getSize() + 
                _fr_size + 
                icm.getSize() + 
                sizeof(bool) + (icm_pok.has_value() ? icm_pok->getSize() : 0) + 
                ctxt.getSize()
                ;
        }

        TxOut() {}

        TxOut(std::istream& in)
            : TxOut()
        {
            in >> *this;
        }

        /**
         * Creates an output for a coin of type 'coin_type' of value 'val', where the PS16 random base is 'H' and the value commitment under (g_3, g) uses randomness 'z'.
         */
        TxOut(
            const CommKey& ck_val, // this is the (g_3, g) CK, and *not* the coin CK
            const IBE::MPK& mpk,
            const RangeProof::Params& rpp,
            const Fr& coin_type,
            const Fr& exp_date,
            const G1& H,
            const std::string& pid,
            const Fr& val,
            const Fr& z,
            bool icmPok,
            bool hasRangeProof
        );

    protected:
        TxOut(
            const CommKey& ck_val, // this is the (g_3, g) CK, and *not* the coin CK
            const IBE::MPK& mpk,
            const RangeProof::Params& rpp,
            const Fr& coin_type,
            const Fr& exp_date,
            const G1& H,
            const std::string& pid,
            const Fr& val,
            const Fr& z,
            bool icmPok,
            bool hasRangeProof,
            // extra delegation parameters
            CommKey ck_tx,
            Fr pid_hash_recip
        );

    public:
        /**
         * Returns a SHA256 hash of all TxOuts in a TXN.
         *  TODO(Crypto): See libutt/hashing.md
         */
        static std::string hashAll(const std::vector<TxOut>& txouts);

        std::string hash() const;

        /**
         * Returns the Pointcheval-Sanders randomness H_j used for this output
         */
        //const G1& getH() const { return ck_tx.g.at(0); }
    };

} // end of namespace libutt
