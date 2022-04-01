
public:
ZKSoK signatureOfKnowledge(unsigned char* message, size_t len);

/**
 * A zero-knowledge signature of knowledge of the secrets in a CoinSecrets object.
 * This signature is computed over the hash of all TXN outputs.
 */
class ZKSoK {
 public:
  Fr e;
  Fr s;

 public:
  /**
   * Verifies this signature against the coin commitment and the revealed EPK
   */
  bool verify(const EPK& epk, const CoinComm& cc, const unsigned char* message, size_t len) const;
};

ZKSoK CoinSecrets::signatureOfKnowledge(unsigned char* message, size_t len) {
  (void)message;
  (void)len;
  // TODO: hash message, then incorporate into Schnorr hash as extra input

  throw libxutils::NotImplementedException();
}

bool ZKSoK::verify(const EPK& epk, const CoinComm& cc, const unsigned char* message, size_t len) const {
  (void)epk;
  (void)cc;
  (void)message;
  (void)len;
  // TODO: hash message, then incorporate into Schnorr verification as extra input

  return false;
}
