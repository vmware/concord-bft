#pragma once

#include "Serializable.h"
#include "InternalBFTClient.h"

class KeyManager {
 public:
  struct KeyExchangeMsg : public concord::serialize::SerializableFactory<KeyExchangeMsg> {
    std::string key;
    std::string signature;
    int repID;
    KeyExchangeMsg(){};
    KeyExchangeMsg(std::string key, std::string signature, int repID);
    std::string toString() const;
    static KeyExchangeMsg deserializeMsg(const char* serStr, const int& size);

   protected:
    const std::string getVersion() const;
    void serializeDataMembers(std::ostream& outStream) const;
    void deserializeDataMembers(std::istream& inStream);
  };

 private:
  int repID_{};
  uint32_t clusterSize_{};
  std::set<NodeIdType> exchangedReplicas_;  // TODO to/from Storage
  std::string generateCid();
  // Raw pointer is ok, since this class does not manage this resource.
  InternalBFTClient* client_{nullptr};

  KeyManager(InternalBFTClient* cl, const int& id, const uint32_t& clusterSize);
  KeyManager(const KeyManager&) = delete;
  KeyManager(const KeyManager&&) = delete;
  KeyManager& operator=(const KeyManager&) = delete;
  KeyManager& operator=(const KeyManager&&) = delete;

 public:
  std::atomic_bool keysExchanged{false};
  void sendKeyExchange();
  std::string onKeyExchange(const KeyExchangeMsg& kemsg);
  void onCheckpoint(const int& num);

  static KeyManager& get(InternalBFTClient* cl = nullptr, const int id = 0, const uint32_t clusterSize = 0) {
    static KeyManager km{cl, id, clusterSize};
    return km;
  }
};
