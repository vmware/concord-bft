#pragma once

#include "Serializable.h"
#include "IBasicClient.h"

class KeyManager {
 public:
  struct KeyExchangeMsg : public concord::serialize::SerializableFactory<KeyExchangeMsg> {
    std::string key;
    std::string signature;
    uint32_t repID;
    std::string toString() const;
    static KeyExchangeMsg deserializeMsg(const char* serStr, const uint32_t& size);

   protected:
    const std::string getVersion() const;
    void serializeDataMembers(std::ostream& outStream) const;
    void deserializeDataMembers(std::istream& inStream);
  };

 private:
  bftEngine::IBasicClient* cl_{nullptr};
  int repID_{};
  KeyManager() {}
  KeyManager(const KeyManager&) = delete;
  KeyManager(const KeyManager&&) = delete;
  KeyManager& operator=(const KeyManager&) = delete;
  KeyManager& operator=(const KeyManager&&) = delete;

 public:
  std::atomic_bool keysExchanged{false};
  void set(bftEngine::IBasicClient* cl, const int& id);
  void sendKeyExchange();
  std::string onKeyExchange(const KeyExchangeMsg& kemsg);
  void onCheckpoint(const int& num);

  static KeyManager& get() {
    static KeyManager km;
    return km;
  }
};
