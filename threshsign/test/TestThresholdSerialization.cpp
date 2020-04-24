// Concord
//
// Copyright (c) 2019 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these sub-components is subject to the
// terms and conditions of the sub-component's license, as noted in the
// LICENSE file.

#include "threshsign/Configuration.h"
#include "threshsign/bls/relic/Library.h"
#include "threshsign/bls/relic/BlsPublicParameters.h"
#include "TestThresholdBls.h"
#include "app/RelicMain.h"

#include <map>
#include <set>
#include <vector>
#include <string>
#include <cassert>
#include <memory>
#include <stdexcept>
#include <inttypes.h>

#include "Logger.hpp"
#include "Utils.h"
#include "Timer.h"
#include "XAssert.h"
#include "Logger.hpp"

using namespace std;
using namespace BLS::Relic;
using namespace concord::serialize;

const char secretKeyValue[] =
    "308204BA020100300D06092A864886F70D0101010500048204A4308204A00201000282010100C55B8F7979BF24B335017082BF33EE2960E3A"
    "068DCDB45CA3017214BFB3F32649400A2484E2108C7CD07AA7616290667AF7C7A1922C82B51CA01867EED9B60A57F5B6EE33783EC258B2347"
    "488B0FA3F99B05CFFBB45F80960669594B58C993D07B94D9A89ED8266D9931EAE70BB5E9063DEA9EFAF744393DCD92F2F5054624AA048C7EE"
    "50BEF374FCDCE1C8CEBCA1EF12AF492402A6F56DC9338834162F3773B119145BF4B72672E0CF2C7009EBC3D593DFE3715D942CA8749771B48"
    "4F72A2BC8C89F86DB52ECC40763B6298879DE686C9A2A78604A503609BA34B779C4F55E3BEB0C26B1F84D8FC4EB3C79693B25A77A158EF882"
    "92D4C01F99EFE3CC912D09B020111028201001D05EF73BF149474B4F8AEA9D0D2EE5161126A69C6203EF8162184E586D4967833E1F9BF56C8"
    "9F68AD35D54D99D8DB4B7BB06C4EFD95E840BBD30C3FD7A5E890CEF6DB99E284576EEED07B6C8CEBB63B4B80DAD2311D1A706A5AC95DE768F"
    "017213B896B9EE38D2E3C2CFCE5BDF51ABD27391761245CDB3DCB686F05EA2FF654FA91F89DA699F14ACFA7F0D8030F74DBFEC28D55C902A2"
    "7E9C03AB1CA2770EFC5BE541560D86FA376B1A688D92124496BB3E7A3B78A86EBF1B694683CDB32BC49431990A18B570C104E47AC6B0DE561"
    "6851F4309CFE7D0E20B17C154A3D85F33C7791451FFF73BFC4CDC8C16387D184F42AD2A31FCF545C3F9A498FAAC6E94E902818100F40CF915"
    "2ED4854E1BBF67C5EA185C52EBEA0C11875563AEE95037C2E61C8D988DDF71588A0B45C23979C5FBFD2C45F9416775E0A644CAD46792296FD"
    "C68A98148F7BD3164D9A5E0D6A0C2DF0141D82D610D56CB7C53F3C674771ED9ED77C0B5BF3C936498218176DC9933F1215BC831E0D4128561"
    "1F512F68327E4FBD9E5C5902818100CF05519FD69D7C6B61324F0A201574C647792B80E5D4D56A51CF5988927A1D54DF9AE4EA656AE259619"
    "23A0EC046F1C569BAB53A64EB0E9F5AB2ABF1C9146935BA40F75E0EB68E0BE4BC29A5A0742B59DF5A55AB028F1CCC42243D2AEE4B74344CA3"
    "3E72879EF2D1CDD874A7F237202AC7EB57AEDCBD539DEFDA094476EAE613028180396C76D7CEC897D624A581D43714CA6DDD2802D6F2AAAE0"
    "B09B885974533E514D6167505C620C51EA41CA70E1D73D43AA5FA39DA81799922EB3173296109914B98B2C31AAE515434E734E28ED31E8D37"
    "DA99BA11C2E693B6398570ABBF6778A33C0E40CC6007E23A15C9B1DE6233B6A25304B91053166D7490FCD26D1D8EAC5102818079C6E4B8602"
    "0674E392CA6F6E5B244B0DEBFBF3CC36E232F7B6AE95F6538C5F5B0B57798F05CFD9DFD28D6DB8029BB6511046A9AD1F3AE3F9EC37433DFB1"
    "A74CC7E9FAEC08A79ED9D1D8187F8B8FA107B08F7DAFE3633E1DCC8DC9A0C8689EB55A41E87F9B12347B6A06DB359D89D6AFC0E4CA2A9FF6E"
    "5E46EF8BA2845F396650281802A89B2BD4A665A0F07DCAFA6D9DB7669B1D1276FC3365173A53F0E0D5F9CB9C3E08E68503C62EA73EB8E0DA4"
    "2CCF6B136BF4A85B0AC424730B4F3CAD8C31D34DD75EF2A39B6BCFE3985CCECC470CF479CF0E9B9D6C7CE1C6C70D853728925326A22352DF7"
    "3B502D4D3CBC2A770DE276E1C5953DF7A9614C970C94D194CAE9188";
const char publicKeyValue[] =
    "0312651421b08cc9140e34471ed2b3a1c12e60e9bd55a4a1f78842bc06d80d6ac31598ee4cb26df7c81d30208ff2d065249a490634ed4e61a"
    "5f19de1e203334339";
const char verificationKeyValue1[] =
    "031ef8af0a33cd7b42a0b853847d9f275e8bab88dbe753b668309883a3e962ed72156ecabe1b83dcafbcef438bb334366f4e3e83f6b2564f3"
    "e02f1f4a670ec36fa";
const char verificationKeyValue2[] =
    "03102c402140a917648f5be446e65fcf0eee2a9cc3d2f8a9489b98997b152cc1010381ff14e508ef7d0f01346805e8ad1f396dfa218ea525f"
    "695debc4bf2d43f8d";
const char verificationKeyValue3[] =
    "03172b38140843bfd7fe63b55d13045effcf597bc9e003102e4e160c74a9e3fd6f11e38cd307a23afd1da250f72f4e422d9863a8c3db71381"
    "432a5cf4171e50609";
const char verificationKeyValue4[] =
    "02143bb5256bc80e9e1f048ef4c42f0c5e27f16e345b58482e0a4adf77b235d41d1e2c4b0636edf13b853f21b0ec738b70d47837389832498"
    "ecbea82c878ecb5ba";

const int numOfSigners = 3;

bool testBlsThresholdSigner(const BlsPublicParameters &params) {
  ShareID id = 0x208419;
  BNT secretKey(secretKeyValue);
  SerializablePtr origSigner(new BlsThresholdSigner(params, id, secretKey));
  std::stringstream strstr;

  origSigner->serialize(strstr);
  Serializable *resultSigner = nullptr;
  BlsThresholdSigner::deserialize(strstr, resultSigner);

  auto *inSigner = dynamic_cast<BlsThresholdSigner *>(origSigner.get());
  auto *outSigner = dynamic_cast<BlsThresholdSigner *>(resultSigner);
  auto returnVal = (resultSigner && (*inSigner == *outSigner));
  delete resultSigner;
  return returnVal;
}

vector<BlsPublicKey> prepareVerificationKeysVector() {
  vector<BlsPublicKey> verificationKeys;
  const G2T verificationKey1(verificationKeyValue1);
  const G2T verificationKey2(verificationKeyValue2);
  const G2T verificationKey3(verificationKeyValue3);
  const G2T verificationKey4(verificationKeyValue4);
  verificationKeys.emplace_back(BlsPublicKey(verificationKey1));
  verificationKeys.emplace_back(BlsPublicKey(verificationKey2));
  verificationKeys.emplace_back(BlsPublicKey(verificationKey3));
  verificationKeys.emplace_back(BlsPublicKey(verificationKey4));
  return verificationKeys;
}

void printRawBuf(const UniquePtrToChar &buf, int64_t bufSize) {
  for (int i = 0; i < bufSize; ++i) {
    char c = buf.get()[i];
    if (c >= 48 && c <= 57)
      printf("%d\n", c);
    else
      printf("%c\n", c);
  }
}

bool testBlsThresholdVerifier(const BlsPublicParameters &params, const vector<BlsPublicKey> &verificationKeys) {
  G2T publicKey(publicKeyValue);

  SerializablePtr origVerifier(
      new BlsThresholdVerifier(params, publicKey, numOfSigners, numOfSigners, verificationKeys));
  std::stringstream strstr;
  origVerifier->serialize(strstr);
  Serializable *resultVerifier(nullptr);
  BlsThresholdVerifier::deserialize(strstr, resultVerifier);

  auto *inVerifier = dynamic_cast<BlsThresholdVerifier *>(origVerifier.get());
  auto *outVerifier = dynamic_cast<BlsThresholdVerifier *>(resultVerifier);
  auto returnVal = (resultVerifier && (*inVerifier == *outVerifier));
  delete resultVerifier;
  return returnVal;
}

bool testBlsMultisigVerifier(const BlsPublicParameters &params, const vector<BlsPublicKey> &verificationKeys) {
  SerializablePtr origVerifier(new BlsMultisigVerifier(params, numOfSigners, numOfSigners, verificationKeys));

  std::stringstream strstr;
  origVerifier->serialize(strstr);

  Serializable *resultVerifier(nullptr);
  BlsMultisigVerifier::deserialize(strstr, resultVerifier);

  auto *inVerifier = dynamic_cast<BlsMultisigVerifier *>(origVerifier.get());
  auto *outVerifier = dynamic_cast<BlsMultisigVerifier *>(resultVerifier);
  auto returnVal = (resultVerifier && (*inVerifier == *outVerifier));
  delete resultVerifier;
  return returnVal;
}

int RelicAppMain(const Library &lib, const vector<string> &args) {
  (void)args;
  (void)lib;
  // uncomment if needed
  // log4cplus::Logger::getInstance( LOG4CPLUS_TEXT("serializable")).setLogLevel(log4cplus::TRACE_LOG_LEVEL);
  BlsPublicParameters params(PublicParametersFactory::getWhatever());

  assertTrue(testBlsThresholdSigner(params));
  vector<BlsPublicKey> verificationKeys = prepareVerificationKeysVector();
  assertTrue(testBlsThresholdVerifier(params, verificationKeys));
  assertTrue(testBlsMultisigVerifier(params, verificationKeys));

  return 0;
}
