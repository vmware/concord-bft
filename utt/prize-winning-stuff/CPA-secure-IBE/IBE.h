#pragma once

#include <iostream>

#include <utt/PolyCrypto.h>

#include <xutils/AutoBuf.h>

// WARNING: For serialization, see operator<< and operator>> declarations at the end of this file.

namespace libutt {

    class IBE {
    public:
        /**
         * Public parameters for the IBE scheme.
         */
        class Params {
        public:
            G1 g1;
            G2 g2;
   
        public:
            static Params random() {
                Params p;
                p.g1 = G1::random_element();
                p.g2 = G2::random_element();
                return p;
            }

            template<class Group>
            static Group hash1(const std::string& id) {
                return hashToGroup<Group>("ibe|G1-or-G2|" + id);
            }
            
            //static void hash2(const GT& elem, unsigned char * buf, size_t capacity) {
            //    (void)elem;
            //    (void)buf;
            //    (void)capacity;
            //}

        public:
            bool operator==(const Params& o) const {
                return g1 == o.g1 &&
                    g2 == o.g2;
            }

            bool operator!=(const Params& o) const {
                return !operator==(o);
            }
        };

        /**
         * A key-private encryption of the coin commitment randomness and its value.
         *
         * TODO: Make this agnostic of plaintext so it can encrypt anything? 
         * Right now assuming plaintext is two Fr's. See methods below too.
         */
        class Ctxt {
        public:
            constexpr static size_t IV_SIZE = 16;
            constexpr static size_t KEY_SIZE = 32;

        public:
            // g_1^r, where r is randomly picked by the sender
            G1 R;

            // AES initialization vector
            unsigned char iv[IV_SIZE];

            // AES ciphertext, encrypted under H(g^{r ask.enc})
            AutoBuf<unsigned char> buf;

        public:
            // WARNING: We only use this to create an empty object for deserializing into
            Ctxt() {}

            Ctxt(size_t size) : buf(size) {}

            Ctxt(std::istream& in);

        public:
            bool operator==(const Ctxt& o) const;
            bool operator!=(const Ctxt& o) const {
                return !operator==(o);
            }

        public:
            static size_t getCtxtSize(size_t ptxtSize);
            
            // TODO: as mentioned above, should work for any plaintext rather than assume we're always encrypting three Fr's
            static size_t getPtxtSize();
            static size_t getCtxtSize() { return getCtxtSize(getPtxtSize()); }
        };

        /**
         * The IBE authority's master PK
         */
        class MPK {
        public:
            IBE::Params p;      // public parameter
            G1 mpk;             // g_1^msk

        public:
            /**
             * Returns a key-private encryption of the coin's value and commitment randomness.
             * Only the user who knows the secret key for 'pid' can decrypt (i.e., secrecy).
             * Furthermore, nobody can tell that the ciphertext is for the user with identifier 'pid' (i.e., anonymity or key-privacy).
             */
            Ctxt encrypt(const std::string& pid, const Fr& val, const Fr& vcm_r, const Fr& icm_r) const;

            AutoBuf<unsigned char> deriveOneTimeKey(const std::string& pid, const Fr& r) const {
                // i.e., e(mpk, H_1(pid))^r = e(g_1, g_2)^{r h \msk}
                GT gt = ReducedPairing(r * mpk, IBE::Params::hash1<G2>(pid));

                auto buf = hashToBytes(gt);
                assertEqual(buf.size(), IBE::Ctxt::KEY_SIZE);
                return buf;
            }
        };

        /**
         * Encryption SK for a user, derived from IBE authority's MSK (see below)
         */
        class EncSK {
        public:
            G2 encsk;           // H_1(id)^\msk \in G2

        public:
            /**
             * Decrypts the coin value and the value commitment randomness and the identity commitment randomness.
             */
            std::tuple<Fr, Fr, Fr> decrypt(const IBE::Ctxt& ctxt) const;

            AutoBuf<unsigned char> deriveOneTimeKey(const G1& R) const {
                // i.e., e(g_1^r, H_1(pid)^\msk) = e(g_1, g_2)^{r h \msk}
                GT gt = ReducedPairing(R, encsk);

                auto buf = hashToBytes(gt);
                assertEqual(buf.size(), IBE::Ctxt::KEY_SIZE);
                return buf;
            }
        };

        /**
         * The IBE authority's master PK
         */
        class MSK {
        protected:
            // the master SK of the IBE authority
            Fr msk;

        public:
            static MSK random() { 
                MSK msk;
                msk.msk = Fr::random_element(); 
                return msk;
            }

        public:
            // Returns the encryption SK for the user with identifier id
            EncSK deriveEncSK(const std::string id) const {
                EncSK encsk;
                encsk.encsk = msk * IBE::Params::hash1<G2>(id);
                return encsk;
            }

            // Currently, IBE public parameters are just g2 \in G_2
            MPK toMPK(const IBE::Params& p) const {
                MPK mpk;
                mpk.p = p;
                mpk.mpk = msk * p.g1;
                return mpk;
            }
        };
    };

} // end of namespace libutt

std::ostream& operator<<(std::ostream&, const libutt::IBE::Params&);
std::istream& operator>>(std::istream&, libutt::IBE::Params& );

std::ostream& operator<<(std::ostream&, const libutt::IBE::MPK&);
std::istream& operator>>(std::istream&, libutt::IBE::MPK& );

// WARNING: Serialization and deserialization operators, must be in the global namespace
std::ostream& operator<<(std::ostream&, const libutt::IBE::Ctxt&);
std::istream& operator>>(std::istream&, libutt::IBE::Ctxt& );

std::ostream& operator<<(std::ostream&, const libutt::IBE::EncSK&);
std::istream& operator>>(std::istream&, libutt::IBE::EncSK& );
