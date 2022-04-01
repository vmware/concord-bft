#pragma once

#include <iostream>
#include <vector>

#include <utt/PolyCrypto.h>

// WARNING: Forward declaration(s), needed for the serialization declarations below
namespace libutt {
    class Comm;
    class CommKey;
}

// WARNING: Declare (de)serialization stream operators in the *global* namespace
std::ostream& operator<<(std::ostream&, const libutt::Comm&);
std::istream& operator>>(std::istream&, libutt::Comm&);

// WARNING: Declare (de)serialization stream operators in the *global* namespace
std::ostream& operator<<(std::ostream&, const libutt::CommKey&);
std::istream& operator>>(std::istream&, libutt::CommKey&);

namespace libutt {

    /**
     * The commitment key (i.e., common reference string) used to compute
     * a Pedersen commitment in G1 and G2. 
     */
    class CommKey {
    public:
        // NOTE: The last group element in the vector is the randomness base!
        std::vector<G1> g;          // i.e., g_1, g_2, \dots, g_n, g
        // some CKs don't have this set and we will fail miserably in that case
        std::vector<G2> g_tilde;    // i.e., \tilde{g}_1, \dots, \tilde{g}_2, \tilde{g}

    public:
        size_t getSize() const {
            return 
                sizeof(size_t) + (_g1_size * g.size()) +
                sizeof(size_t) + (_g2_size * g_tilde.size());
        }

        CommKey()
        {}

        CommKey(const std::vector<G1>& g)
            : g(g)
        {}

    public:
        /**
         * Returns a randomly-generated commitment key for committing to n messages.
         * So it will consist of 2(n + 1) group elements.
         */
        static CommKey random(size_t n);
        static CommKey fromTrapdoor(const G1& g, const G2& g_tilde, const std::vector<Fr>& y);

    public:
        // Returns the # of messages this CK can be used to commit to
        // e.g, if CK is (g_1, g_2, g), returns 2
        size_t numMessages() const { testAssertFalse(g.empty()); return g.size() - 1; }

        // g1(i) returns g_i
        const G1& g1(size_t i) const { return g.at(i); }
        // g2(i) returns \tilde{g}_i
        const G2& g2(size_t i) const { return g_tilde.at(i); }

        const G1& getGen1() const { return g.back(); }
        const G2& getGen2() const { testAssertFalse(g_tilde.empty()); return g_tilde.back(); }

        bool hasG2() const { return !g_tilde.empty(); }
        
        bool hasCorrectG2() const {
            testAssertTrue(hasG2());

            if(g.size() != g_tilde.size())
                return false;

            for(size_t i = 0; i < g.size() - 1; i++)
            {
                if(ReducedPairing(g[i], getGen2()) != ReducedPairing(getGen1(), g_tilde.at(i))) {
                    logerror << "CK is inconsistent at g_" << i+1 << endl;
                    return false;
                }
            }

            return true;
        }

        /**
         * Used when hashing a CK during a \Sigma-protocol proof.
         */
        std::string toString() const;

    public:
        bool operator==(const CommKey& ck) const {
            return g == ck.g && g_tilde == ck.g_tilde;
        }

        bool operator!=(const CommKey& ck) const {
            return !operator==(ck);
        }
    };
    
    /**
     * A "bilinear" Pedersen commitment to a vector of messages.
     * This is a commitment in bilinear groups G1 and G2 (optional).
     *
     * Typically, this is a commitment to a coin.
     *      
     *      g_1^s g_2^sn g_3^val g^r, where:
     *
     *  's' is the owner's address secret key (i.e., old AddrSK)
     *  'val' is the coin's value or denomination
     *  'sn' is the coin's serial number, used to distinguish it from other
     *  coins of same value 'val' also owned by 's'
     *
     * Other times, this is a commitment to either a zerocoin (as g_1^s g^t) or 
     * to a value (as g_3^val g^z).
     */
    class Comm {
    public:
        G1 ped1;

        /**
         * In order to verify PS signatures on Pedersen commitments, we need to
         * have the same commitment, but in G2.
         * This is why this is here.
         *
         * However, I foresee we will only need this for the input coin 
         * commitments, whose signature is being verified by the bank.
         *
         * For the ouput coin commitments, created by the sender, I think we
         * might be able to get away in the security proof w/o including the 
         * G2 counterparts.
         *
         * (This is because, roughly speaking, there will be an implicit
         * "for-free" PoK on output coin commitments via their zerocoin 
         * signature and their range proof.)
         *
         * Hence the std::optional<G2> here.
         */
        std::optional<G2> ped2; 

    public:
        size_t getSize() const {
            return _g1_size +
                sizeof(bool) + (hasG2() ? _g2_size : 0);
        }
        // WARNING: We only use this to create an empty object for deserializing into
        Comm() {}

        // Deserializes a Comm from an istream
        Comm(std::istream& in)
            : Comm()
        {
            in >> *this;
        }

        Comm(const G1& ped1) : ped1(ped1) {}

    public:
        /**
         * Commits to the specified messages (and randomness), potentially computing the G2 counterpart too.
         * 
         * The last message in m is the randomness.
         */
        static Comm create(const CommKey& ck, const std::vector<Fr>& m, bool withG2);

        /**
         * Commits to the specified messages individually, as needed for the threshold PS16 signing 
         * (see RandSigShareSK::shareSign() in RandSig.h).
         *
         * Specifically, if ck = (h, g), then cm_i = h^{m_i} g^{r_i}
         */
        static std::vector<Comm> create(const CommKey& ck, const std::vector<Fr>& m, const std::vector<Fr>& r, bool withG2);

    public:
        G1 asG1() const { return ped1; }
        G2 asG2() const { testAssertTrue(hasG2()); return *ped2; }

        bool hasG2() const { return ped2.has_value(); }

        /**
         * Checks that the G2 counterpart of the commitment is set and is indeed
         * a commitment to the same message.
         */
        bool hasCorrectG2(const CommKey& ck) const;

        void rerandomize(const CommKey& ck, const Fr& r_delta);

        // WARNING: For hashing, **not** for serializing, Comms; because does not include G2 counterpart in output
        std::string toString() const {
            std::stringstream ss;
            ss << ped1;
            return ss.str();
        }

    public:
        bool operator==(const Comm& other) const {
            if(ped2.has_value() && other.ped2.has_value() && *ped2 != *other.ped2)
                return false;

            return ped1 == other.ped1;
        }

        bool operator!=(const Comm& other) const {
            return !operator==(other);
        }

        Comm& operator+=(const Comm& rhs) {
            ped1 = ped1 + rhs.ped1;
            if(ped2.has_value()) {
                testAssertTrue(rhs.ped2.has_value());
                (*ped2) = (*ped2) + *rhs.ped2;
            }
            return *this;
        }

        // friends defined inside class body are inline and are hidden from non-ADL lookup
        // passing lhs by value helps optimize chained a+b+c
        friend Comm operator+(Comm lhs, const Comm& rhs) {
            lhs += rhs;
            return lhs;
        }
    };

} // end of namespace libutt
