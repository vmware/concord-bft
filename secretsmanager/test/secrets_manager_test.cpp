#include "gtest/gtest.h"
#include "base64.h"
#include "openssl_pass.h"
#include "aes.h"
#include "secrets_manager_enc.h"

using namespace concord::secretsmanager;

const std::string long_input{R"L0R3M(

Lorem ipsum dolor sit amet, consectetur adipiscing elit. Maecenas ut ultrices nisi. Sed eu venenatis tellus. Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia curae; Sed ante tellus, auctor non feugiat et, feugiat vitae ante. Pellentesque volutpat tincidunt orci non efficitur. Vestibulum eu sagittis nisi, et faucibus neque. Nullam eu ultrices dolor. Nulla facilisi. Lorem ipsum dolor sit amet, consectetur adipiscing elit. Curabitur luctus lectus non neque sollicitudin facilisis. Curabitur dapibus, lorem eget lacinia luctus, eros velit interdum odio, non efficitur massa ipsum id orci. Morbi sagittis enim neque, et blandit arcu vehicula eget. Aliquam lacinia lacus at metus elementum pretium. Aenean efficitur nisl ut arcu sodales gravida. Cras malesuada magna ac eros pharetra feugiat.

Phasellus massa ante, consequat ut ex sed, vestibulum scelerisque orci. Phasellus tristique, odio eget pulvinar tempus, sem nisi iaculis purus, nec faucibus ligula mauris vitae lacus. Proin aliquam sollicitudin hendrerit. Suspendisse at sapien fringilla nunc blandit dignissim. Praesent eget cursus ipsum, rhoncus cursus orci. Integer at pulvinar quam, quis volutpat ipsum. Pellentesque lorem purus, aliquam sed imperdiet nec, viverra in metus. Pellentesque eleifend fringilla magna, ut auctor elit dapibus in. Proin leo nunc, scelerisque vel ante non, egestas placerat tellus. Class aptent taciti sociosqu ad litora torquent per conubia nostra, per inceptos himenaeos. Ut et venenatis elit. Nunc malesuada lacus sed purus tincidunt, non dictum lacus malesuada.

Aliquam id nunc sed mi lacinia efficitur. Maecenas a ex eget nunc congue laoreet ac quis mi. Donec id est turpis. Donec viverra tincidunt mi, id rutrum justo faucibus at. Quisque vel nunc tortor. Suspendisse at velit ipsum. Maecenas malesuada justo suscipit tortor tempus tincidunt. Vestibulum erat elit, fermentum ac nunc et, bibendum finibus neque. Morbi non urna gravida, aliquet nibh vel, aliquam est. Lorem ipsum dolor sit amet, consectetur adipiscing elit. Phasellus semper dui eu nisi consectetur finibus. Nam ac ante urna.

Nullam dignissim malesuada pharetra. Maecenas mollis ligula ut pharetra auctor. Sed purus neque, eleifend vel ligula eget, ultrices cursus turpis. Class aptent taciti sociosqu ad litora torquent per conubia nostra, per inceptos himenaeos. Suspendisse sit amet eleifend enim, a pellentesque erat. Etiam imperdiet leo in magna laoreet ornare. Vestibulum tempus tempor quam, sed cursus metus hendrerit quis. Duis eget venenatis lectus. Aliquam suscipit pretium ligula ut tempus. Ut lobortis tortor et mauris sagittis molestie. Nunc tortor mi, rhoncus id risus quis, tristique luctus nisl.

Aliquam sed venenatis arcu. Phasellus tincidunt odio eget tellus condimentum dictum. Pellentesque mattis sagittis bibendum. Maecenas eu enim non diam aliquam aliquam. Fusce aliquet at felis id ultrices. Proin ultricies eros metus, sit amet dignissim massa pretium eu. Nullam sagittis quis nibh quis sodales. Phasellus turpis turpis, efficitur id consequat at, tempus sit amet urna. Phasellus consequat consectetur nunc, non dignissim lectus. Nam aliquam quam ut faucibus vestibulum. Aliquam laoreet venenatis purus sed tempor. Vivamus id orci lobortis, cursus velit a, cursus mi. Cras pellentesque tincidunt felis, at ullamcorper tortor molestie sed.

Quisque ornare sollicitudin arcu, a dignissim nibh vehicula id. Quisque augue metus, commodo in urna vel, tempor efficitur felis. Sed sed pretium ipsum, in dictum libero. Suspendisse consectetur sodales nunc, aliquam pharetra urna feugiat eget. Vestibulum et tincidunt magna. Phasellus sollicitudin turpis dui, non malesuada dolor lacinia eget. Ut sit amet massa nec elit laoreet tristique vel sed nunc. Donec urna orci, placerat ac sollicitudin venenatis, volutpat in purus. Curabitur massa urna, rhoncus et sem vel, euismod vehicula augue. Sed eu suscipit eros. Maecenas pulvinar condimentum mauris, sit amet facilisis neque placerat sed. Sed id porta ante. Aenean non diam vel nibh vulputate dapibus. In hac habitasse platea dictumst.

Integer vel risus sodales, euismod arcu vitae, aliquet sem. Cras sagittis ligula sem, et scelerisque orci gravida vitae. Sed molestie orci vitae eros vulputate, eu semper massa pellentesque. Nunc hendrerit enim libero, sed congue nisl eleifend id. Nulla rhoncus at quam a pretium. Integer vel fermentum mauris. Nunc ultricies, mi id fermentum efficitur, velit dui vestibulum sem, at maximus diam odio sed diam. Vestibulum in metus euismod, accumsan erat ac, dignissim ante. Aliquam eget malesuada odio, nec hendrerit dolor. Vestibulum lobortis lorem eu arcu fermentum, vehicula suscipit neque lacinia. Ut sit amet porttitor dui. Donec sodales, nibh in vehicula dignissim, arcu eros pharetra arcu, auctor consequat orci felis ut risus. Lorem ipsum dolor sit amet, consectetur adipiscing elit. Proin eu orci posuere nibh blandit placerat et eget nunc. Class aptent taciti sociosqu ad litora torquent per conubia nostra, per inceptos himenaeos.

Aliquam vitae tincidunt sem. Vestibulum vitae pharetra dolor. Praesent sollicitudin semper tellus, non lacinia sem. Aliquam iaculis ex massa, quis blandit urna interdum euismod. Donec sit amet cursus nunc. Nulla nec augue et purus blandit tristique. Nulla blandit libero sed neque tristique, quis ultricies lorem maximus. Aenean vel sollicitudin elit. Nulla interdum finibus dolor quis viverra. Aliquam ultricies tincidunt augue, a cursus urna iaculis pulvinar. Donec vitae neque laoreet, ultrices est id, tempor ipsum.

Sed vitae urna volutpat, hendrerit ante eget, convallis turpis. Cras dapibus nunc nec convallis bibendum. Suspendisse egestas eget velit a dictum. Vivamus aliquet felis vitae lorem blandit euismod. Sed rutrum dolor ac est finibus eleifend. Sed at arcu in purus mattis commodo. Sed ac turpis arcu. Donec ac tellus sit amet orci venenatis commodo. Orci varius natoque penatibus et magnis dis parturient montes, nascetur ridiculus mus. Nunc et aliquet justo. Fusce nec malesuada eros.

Aenean laoreet sit amet augue quis suscipit. Fusce nec consequat mauris. Pellentesque vel imperdiet turpis, quis sodales est. Suspendisse scelerisque velit sit amet libero bibendum cursus. Donec a mollis tortor. Vestibulum blandit a mi et malesuada. Phasellus pulvinar eu metus quis placerat. Cras sodales libero a justo varius finibus. Duis tempor rutrum ornare. Etiam luctus semper vulputate. Fusce at dictum velit, et aliquam est. Mauris pellentesque lectus at nunc bibendum, et volutpat purus congue. Vestibulum porttitor, metus a convallis mollis, odio dui maximus lacus, id ultricies erat felis et sapien.

Aenean pharetra bibendum dui non accumsan. Vivamus ex tellus, accumsan tincidunt tempus sed, cursus eu mi. Ut feugiat ligula nec egestas tincidunt. Morbi egestas viverra tellus, a. )L0R3M"};

TEST(SecretsManagerEnc, EndToEnd) {
  // The values below are generated with openssl:
  // $ openssl enc -base64 -debug -aes-256-cbc -e -in ./sample.txt -md sha256 -pass pass:XaQZrOYEQw -p -out sample.enc
  const std::string input{"This is a sample text"};
  const std::vector<uint8_t> salt{0x50, 0x54, 0xB7, 0xAF, 0x35, 0xB8, 0xB6, 0xED};
  const std::string password{"XaQZrOYEQw"};
  const std::string salt_prefix{"Salted__"};
  const std::string encrypted{"U2FsdGVkX19QVLevNbi27WbILeleXn5BkWHg3A80UkcEIDFRQPN1ODx/eqZ3UEA4\n"};

  // Derive key from password - openssl style (-pass option)
  auto key_params = deriveKeyPass(password, salt, 256 / 8, 16);

  {
    std::stringstream key_str;
    for (int k : key_params.key) {
      key_str << std::setfill('0') << std::setw(2) << std::right << std::hex << k;
    }
    ASSERT_EQ(key_str.str(), "ba502b2803c6c4270b8530b24b9147fad46fe57931410a49920e5058794133c4");
  }

  {
    std::stringstream iv_str;
    for (int i : key_params.iv) {
      iv_str << std::setfill('0') << std::setw(2) << std::right << std::hex << i;
    }
    ASSERT_EQ(iv_str.str(), "4a72528785658c36aa7e329e9a65174d");
  }

  // Encrypt
  AES_CBC e(key_params);
  auto cipher_text = e.encrypt(input);
  auto cipher_text_encoded = base64Enc(salt, cipher_text);

  ASSERT_EQ(cipher_text_encoded, encrypted);

  // Decrypt
  auto dec = base64Dec(cipher_text_encoded);
  ASSERT_EQ(dec.salt, salt);
  ASSERT_EQ(dec.cipher_text, cipher_text);

  auto plain_text = e.decrypt(dec.cipher_text);
  ASSERT_EQ(plain_text, input);
}

TEST(SecretsManagerEnc, Base64) {
  const std::vector<uint8_t> cipher_text{
      0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F};
  const std::vector<uint8_t> bad_salt{0x01, 0x02, 0x03, 0x04};
  const std::vector<uint8_t> good_salt{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08};

  ASSERT_THROW(base64Enc(bad_salt, cipher_text), std::runtime_error);

  auto e = base64Enc(good_salt, cipher_text);
  auto r = base64Dec(e);

  ASSERT_EQ(r.salt, good_salt);
  ASSERT_EQ(r.cipher_text, cipher_text);
}

TEST(SecretsManagerEnc, FileTest) {
  std::string filename{"/tmp/secrets_manager_unit_test"};

  SecretData ret;
  ret.algo = "AES/CBC/PKCS5Padding";
  ret.digest = "SHA-256";
  ret.key_length = 256;
  ret.password = "XaQZrOYEQw";

  SecretsManagerEnc sm(ret);

  sm.encryptFile(filename, long_input);
  auto output = sm.decryptFile(filename);

  ASSERT_EQ(long_input, output);
}

TEST(SecretsManagerEnc, EmptyInput) {
  SecretData ret;
  ret.algo = "AES/CBC/PKCS5Padding";
  ret.digest = "SHA-256";
  ret.key_length = 256;
  ret.password = "XaQZrOYEQw";

  SecretsManagerEnc sm(ret);
  auto res = sm.decryptString("");
  ASSERT_FALSE(res.has_value());
}