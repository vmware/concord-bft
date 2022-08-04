#include "key_params.h"
#include "hex_tools.h"
#include "ReplicaConfig.hpp"

#include <cstring>

// CryptoPP headers.
#include <cryptopp/cryptlib.h>
#include <cryptopp/filters.h>
#include <cryptopp/hex.h>

namespace concord::secretsmanager {
using bftEngine::ReplicaConfig;
using concord::crypto::SIGN_VERIFY_ALGO;

KeyParams::KeyParams(const std::string& pkey, const std::string& piv) {
  if (ReplicaConfig::instance().replicaMsgSigningAlgo == SIGN_VERIFY_ALGO::RSA) {
    CryptoPP::StringSource sskey(pkey, true, new CryptoPP::HexDecoder(new CryptoPP::VectorSink(key)));
    CryptoPP::StringSource ssiv(piv, true, new CryptoPP::HexDecoder(new CryptoPP::VectorSink(iv)));
  } else if (ReplicaConfig::instance().replicaMsgSigningAlgo == SIGN_VERIFY_ALGO::EDDSA) {
    key = concordUtils::unhex(pkey);
    iv = concordUtils::unhex(piv);
  }
}
}  // namespace concord::secretsmanager
