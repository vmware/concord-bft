#pragma once

#include <optional>
#include <tuple>

#include <utt/Coin.h>
#include <utt/PolyCrypto.h>
#include <utt/SplitProof.h>

#include <xassert/XAssert.h>
#include <xutils/AutoBuf.h>
#include <xutils/Log.h>
#include <xutils/NotImplementedException.h>

namespace libutt {
class TxIn;
}

std::ostream& operator<<(std::ostream&, const libutt::TxIn&);
std::istream& operator>>(std::istream&, libutt::TxIn&);

namespace libutt {

class TxIn {
 public:
  Fr coin_type;
  Fr exp_date;

  Nullifier null;  // nullifier that can be used to keep track of the coin as spent
  Comm vcm;        // commitment to coin's value

  Comm ccm;         // commitment to coin (except coin type and expiration date)
  RandSig coinsig;  // signature on the coin commitment 'ccm'

  SplitProof pi;  // proof that 'ccm' was correctly split into 'null' and 'vcm'

 public:
  size_t getSize() const {
    return _fr_size + _fr_size + null.getSize() + vcm.getSize() + ccm.getSize() + coinsig.getSize() + pi.getSize();
  }
  TxIn() {}

  TxIn(std::istream& in) : TxIn() { in >> *this; }

  /**
   * Creates a TxIn from an already-rerandomized coin
   */
  TxIn(const Coin& c)
      : coin_type(c.type),
        exp_date(c.exp_date),
        null(c.null),
        vcm(c.vcm),
        ccm(c.commForTxn()),
        coinsig(c.sig)
  // pi --> is computed by Tx::Tx()
  {}
};

}  // end of namespace libutt
