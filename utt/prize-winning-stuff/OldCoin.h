#pragma once

#include <utt/CoinSecrets.h>
#include <utt/Comm.h>
#include <utt/Nullifier.h>
#include <utt/RandSig.h>

namespace libutt {

    class CoinSecrets;
    class RandSig;

    /**
     * A coin inside a wallet that the owning user can spend.
     */
    class Coin {
    public:
        CoinSecrets cs; // coin secrets: owner, serial number and value
        Comm cc;        // coin commitment to above coin secrets
        RandSig sig;    // signature on coin commitment from bank

        //
        // NOTE(Alin): Precomputation optimizations!
        //
        Nullifier null; // nullifier for this coin, pre-computed for convenience

        Comm vcm;     // value commitment for this coin, pre-computed for convenience
        Fr z;         // value commitment randomness
    };

}
