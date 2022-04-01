#include <utt/Configuration.h>

#include <utt/Address.h>
#include <utt/IBE.h>
#include <utt/Params.h>

#include <utt/internal/plusaes.h>

#include <xutils/AutoBuf.h>
#include <xutils/Log.h>
#include <xutils/Utils.h>

std::ostream& operator<<(std::ostream& out, const libutt::IBE::Params& p) {
    out << p.g1; 
    out << p.g2;
    return out;
}

std::istream& operator>>(std::istream& in, libutt::IBE::Params& p) {
    in >> p.g1; 
    in >> p.g2;
    return in;
}

std::ostream& operator<<(std::ostream& out, const libutt::IBE::MPK& mpk) {
    out << mpk.p;
    out << mpk.mpk;
    return out;
}

std::istream& operator>>(std::istream& in, libutt::IBE::MPK& mpk) {
    in >> mpk.p;
    in >> mpk.mpk;
    return in;
}

// WARNING: Serialization operators must be in the global namespace
std::ostream& operator<<(std::ostream& out, const libutt::IBE::Ctxt& c) {
    out << c.R << std::endl;
    out << Utils::bin2hex(c.iv, libutt::IBE::Ctxt::IV_SIZE) << std::endl;
    out << Utils::bin2hex(c.buf, c.buf.size()) << endl;

    return out;
}

// WARNING: Deserialization operator must be in the global namespace
std::istream& operator>>(std::istream& in, libutt::IBE::Ctxt& c) {
    in >> c.R;
    libff::consume_OUTPUT_NEWLINE(in);

    std::string ivHex;
    in >> ivHex;

    // SECURITY: Make sure the IV has the right length
    size_t ivSize = libutt::IBE::Ctxt::IV_SIZE;
    if(ivHex.size() != ivSize * 2) {
        logerror << "Actual IV size (hex chars): " << ivHex.size() << std::endl;
        logerror << "Actual IV size (bytes): " << ivHex.size() / 2 << std::endl;
        logerror << "Expected IV size (bytes): " << ivSize << std::endl;

        throw std::runtime_error("Expected AES IV to be of a particular size");
    }

    Utils::hex2bin(ivHex, c.iv, ivSize);

    // SECURITY: Make sure the ctxt has the right length
    std::string ctxtHex;
    in >> ctxtHex;
    size_t ctxtSize = libutt::IBE::Ctxt::getCtxtSize();
    if(ctxtHex.size() !=  ctxtSize * 2) {
        logerror << "Actual ctxt size (hex chars): " << ctxtHex.size() << std::endl;
        logerror << "Actual ctxt size (bytes): " << ctxtHex.size() / 2 << std::endl;
        logerror << "Expected ctxt size (bytes): " << ctxtSize << std::endl;

        throw std::runtime_error("Expected ciphertext size to be of a particular size");
    }

    c.buf = AutoBuf<unsigned char>(ctxtSize);
    Utils::hex2bin(ctxtHex, c.buf, c.buf.size());
    
    return in;
}

std::ostream& operator<<(std::ostream& out, const libutt::IBE::EncSK& sk) {
    out << sk.encsk;
    return out;
}

std::istream& operator>>(std::istream& in, libutt::IBE::EncSK& sk) {
    in >> sk.encsk;
    return in;
}

namespace libutt {

    IBE::Ctxt::Ctxt(std::istream& in)
        : IBE::Ctxt()
    {
        in >> *this;
    }

    size_t IBE::Ctxt::getPtxtSize() {
        size_t frSize = Fr_num_bytes();
        return frSize * 3;
    }

    size_t IBE::Ctxt::getCtxtSize(size_t ptxtSize) {
        return plusaes::get_padded_encrypted_size(ptxtSize);
    }

    IBE::Ctxt IBE::MPK::encrypt(const std::string& pid, const Fr& coinValue, const Fr& vcm_r, const Fr& icm_r) const {
        Fr r = Fr::random_element();

        AutoBuf<unsigned char> enc_key = IBE::MPK::deriveOneTimeKey(pid, r);

        // assemble AES plaintext
        size_t ptxt_size = IBE::Ctxt::getPtxtSize();
        unsigned char plaintext[ptxt_size];

        size_t frSize = Fr_num_bytes();
        Fr_serialize(coinValue, plaintext + 0*frSize, frSize);
        Fr_serialize(vcm_r,     plaintext + 1*frSize, frSize);
        Fr_serialize(icm_r,     plaintext + 2*frSize, frSize);
        
        //logdbg << "Plaintext size in bytes: " << ptxt_size << endl;

        // figure out AES ciphertext size
        size_t ctxt_size = IBE::Ctxt::getCtxtSize(ptxt_size);
        IBE::Ctxt ctxt(ctxt_size);

        // pick random IV for AES encryption
        random_bytes(ctxt.iv, IBE::Ctxt::IV_SIZE);
        //logtrace << "Random IV: " << Utils::bin2hex(ctxt.iv, IBE::Ctxt::IV_SIZE) << std::endl;
        // TODO(Crypto): could probably save ourselves some space and derive IV from 'g_1^r'? need to check security.
        
        // first, store Diffie-Hellman random R in ciphertext
        ctxt.R = r * p.g1;

        // second, encrypt
        plusaes::encrypt_cbc(
            plaintext, ptxt_size,
            enc_key.getBuf(), IBE::Ctxt::KEY_SIZE, 
            &ctxt.iv,
            ctxt.buf.getBuf(), ctxt_size, true);

        //logdbg << "Ciphertext size in bytes: " << ctxt_size << endl;

        return ctxt;
    }

    std::tuple<Fr, Fr, Fr> IBE::EncSK::decrypt(const IBE::Ctxt& ctxt) const {
        // Compute the one-time encryption key
        AutoBuf<unsigned char> enc_key = IBE::EncSK::deriveOneTimeKey(ctxt.R);

        size_t ptxt_size = ctxt.getPtxtSize();
        unsigned char ptxt[ptxt_size];

        // NOTE(Alin): I'm a bit confused, but I think I need to pass padding_size=0 so that 'decrypt_cbc' doesn't return the padding in the decrypted plaintext
        size_t padding_size = 0;
        plusaes::decrypt_cbc(
            ctxt.buf.getBuf(), static_cast<size_t>(ctxt.buf.size()),
            enc_key.getBuf(), static_cast<size_t>(enc_key.size()),
            &ctxt.iv, 
            ptxt, ptxt_size,
            &padding_size);

        // Convert plaintext to Fr's 
        Fr val   = Fr_deserialize(ptxt + 0*Fr_num_bytes(), Fr_num_bytes());
        Fr vcm_r = Fr_deserialize(ptxt + 1*Fr_num_bytes(), Fr_num_bytes());
        Fr icm_r = Fr_deserialize(ptxt + 2*Fr_num_bytes(), Fr_num_bytes());

        return std::make_tuple(val, vcm_r, icm_r);
    }
        
    bool IBE::Ctxt::operator==(const IBE::Ctxt& o) const {
        if(R != o.R) {
            //logdbg << "Different R" << endl;
            return false;
        }

        if(buf != o.buf) {
            //logdbg << "Different ciphertext buf" << endl;
            //logdbg << "  len: " << buf.size() << endl;
            //logdbg << "  o.len: " << o.buf.size() << endl;
            return false;
        }

        if(memcmp(iv, o.iv, IBE::Ctxt::IV_SIZE) != 0) {
            //logdbg << "Different IV" << endl;
            return false;
        }
        
        //logdbg << "Same ciphertext!" << endl;
        return true;
    }

} // end of namespace libutt
