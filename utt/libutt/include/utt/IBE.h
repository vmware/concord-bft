#pragma once

#include <iostream>

#include <utt/PolyCrypto.h>

#include <xutils/AutoBuf.h>

// WARNING: For serialization, see operator<< and operator>> declarations at the end of this file.

namespace libutt {

class IBE {
 public:
  class MPK;

  /**
   * Public parameters for the IBE scheme.
   */
  class Params {
   public:
    constexpr static size_t RAND_NUM_BYTES = 32;  // i.e., l_1 from [Srin10], pg 80

   public:
    G1 g1;

   public:
    static Params random() {
      Params p;
      p.g1 = G1::random_element();
      return p;
    }

    template <class Group>
    static Group hashID(const std::string& id) {
      return hashToGroup<Group>("ibe|G1-or-G2|" + id);
    }

    static Fr hashToR(const IBE::MPK& mpk,
                      const std::string& id,
                      const AutoBuf<unsigned char>& ptxt,
                      const AutoBuf<unsigned char>& sigma);

    static std::tuple<AutoBuf<unsigned char>, AutoBuf<unsigned char>> hashToOneTimeKeys(const GT& T, size_t ptxtSize) {
      auto hashSize = picosha2::k_digest_size;
      auto numHashes = ptxtSize / hashSize;
      if (ptxtSize % hashSize) numHashes++;

      AutoBuf<unsigned char> otk1(numHashes * hashSize);

      for (size_t i = 0; i < numHashes; i++)
        hashToBytes(T, "ibe-otk", std::string("ptxt|") + std::to_string(i), otk1.getBuf() + i * hashSize, hashSize);

      otk1.shrink(ptxtSize);
      assertEqual(otk1.size(), ptxtSize);

      auto otk2 = hashToBytes(T, "ibe-otk", "sigma");
      assertEqual(otk2.size(), IBE::Params::RAND_NUM_BYTES);

      return std::make_tuple(otk1, otk2);
    }

   public:
    bool operator==(const Params& o) const { return g1 == o.g1; }

    bool operator!=(const Params& o) const { return !operator==(o); }
  };

  /**
   * A key-private encryption of the coin commitment randomness and its value.
   */
  class Ctxt {
   public:
    // g_1^r, where r is randomly picked by the sender
    G1 R;

    AutoBuf<unsigned char> buf1;  // i.e., m \xor H_{2,1}(T)
    AutoBuf<unsigned char> buf2;  // i.e., sigma \xor H_{2,2}(T)

   public:
    size_t getSize() const { return _g1_size + sizeof(size_t) + buf1.size() + sizeof(size_t) + buf2.size(); }
    // WARNING: We only use this to create an empty object for deserializing into
    Ctxt() {}

    Ctxt(std::istream& in);

   public:
    bool operator==(const Ctxt& o) const;
    bool operator!=(const Ctxt& o) const { return !operator==(o); }
  };

  /**
   * The IBE authority's master PK
   */
  class MPK {
   public:
    IBE::Params p;  // public parameter
    G1 mpk;         // g_1^msk

   public:
    /**
     * Returns a key-private encryption of the coin's value and commitment randomness.
     * Only the user who knows the secret key for 'pid' can decrypt (i.e., secrecy).
     * Furthermore, nobody can tell that the ciphertext is for the user with identifier 'pid' (i.e., anonymity or
     * key-privacy).
     */
    Ctxt encrypt(const std::string& pid, const AutoBuf<unsigned char>& ptxt) const;

   public:
    bool operator==(const MPK& o) const { return p == o.p && mpk == o.mpk; }

    bool operator!=(const MPK& o) const { return !operator==(o); }
  };

  /**
   * Encryption SK for a user, derived from IBE authority's MSK (see below)
   */
  class EncSK {
   public:
    MPK mpk;
    std::string pid;  // user's PID
    G2 encsk;         // H_1(id)^\msk \in G2

   public:
    /**
     * Decrypts the coin value and the value commitment randomness and the identity commitment randomness.
     */
    std::tuple<bool, AutoBuf<unsigned char>> decrypt(const IBE::Ctxt& ctxt) const;

   public:
    bool operator==(const EncSK& o) const { return mpk == o.mpk && pid == o.pid && encsk == o.encsk; }

    bool operator!=(const EncSK& o) const { return !operator==(o); }
  };

  /**
   * The IBE authority's master PK
   */
  class MSK {
   public:
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
    EncSK deriveEncSK(const IBE::Params& p, const std::string id) const {
      EncSK encsk;
      encsk.pid = id;
      encsk.encsk = msk * IBE::Params::hashID<G2>(id);
      encsk.mpk = toMPK(p);
      return encsk;
    }

    // Currently, IBE public parameters are just g1 \in G_1
    MPK toMPK(const IBE::Params& p) const {
      MPK mpk;
      mpk.p = p;
      mpk.mpk = msk * p.g1;
      return mpk;
    }
  };
};

}  // end of namespace libutt

std::ostream& operator<<(std::ostream&, const libutt::IBE::Params&);
std::istream& operator>>(std::istream&, libutt::IBE::Params&);

std::ostream& operator<<(std::ostream&, const libutt::IBE::MPK&);
std::istream& operator>>(std::istream&, libutt::IBE::MPK&);

std::ostream& operator<<(std::ostream&, const libutt::IBE::MSK&);
std::istream& operator>>(std::istream&, libutt::IBE::MSK&);

// WARNING: Serialization and deserialization operators, must be in the global namespace
std::ostream& operator<<(std::ostream&, const libutt::IBE::Ctxt&);
std::istream& operator>>(std::istream&, libutt::IBE::Ctxt&);

std::ostream& operator<<(std::ostream&, const libutt::IBE::EncSK&);
std::istream& operator>>(std::istream&, libutt::IBE::EncSK&);
