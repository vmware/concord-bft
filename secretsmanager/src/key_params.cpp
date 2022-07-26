#include "key_params.h"
#include <cstring>

#ifdef USE_CRYPTOPP_RSA
#include <cryptopp/cryptlib.h>
#include <cryptopp/filters.h>
#include <cryptopp/hex.h>
#elif USE_EDDSA_SINGLE_SIGN
#include "hex_tools.h"
#endif

namespace concord::secretsmanager {

KeyParams::KeyParams(const std::string& pkey, const std::string& piv) {
#ifdef USE_CRYPTOPP_RSA
  CryptoPP::StringSource sskey(pkey, true, new CryptoPP::HexDecoder(new CryptoPP::VectorSink(key)));
  CryptoPP::StringSource ssiv(piv, true, new CryptoPP::HexDecoder(new CryptoPP::VectorSink(iv)));
#elif USE_EDDSA_SINGLE_SIGN
  key = concordUtils::unhex(pkey);
  iv = concordUtils::unhex(piv);
#endif
}
}  // namespace concord::secretsmanager
