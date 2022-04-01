

public:
/**
 * The ciphertext encryption key is a DH shared key obtained from the recipient's encryption key and a random key picked
 * by the sender.
 */
static AutoBuf<unsigned char> dhKex(const G1& pk1, const Fr& sk2) {
  G1 dh = sk2 * pk1;

  // hash DH key to bytes
  AutoBuf<unsigned char> enc_key(Ctxt::KEY_SIZE);
  hashToBytes(dh, enc_key.getBuf(), Ctxt::KEY_SIZE);

  return enc_key;
}
