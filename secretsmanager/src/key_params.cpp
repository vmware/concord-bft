#include "key_params.h"

#include <cryptopp/cryptlib.h>
#include <cryptopp/filters.h>
#include <cryptopp/hex.h>

namespace concord::secretsmanager {
KeyParams::KeyParams(const std::string& pkey, const std::string& piv) {
  CryptoPP::StringSource sskey(pkey, true, new CryptoPP::HexDecoder(new CryptoPP::VectorSink(key)));
  CryptoPP::StringSource ssiv(piv, true, new CryptoPP::HexDecoder(new CryptoPP::VectorSink(iv)));
}
}  // namespace concord::secretsmanager