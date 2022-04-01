#include <utt/Configuration.h>

#include <utt/Simulation.h>

#include <functional>

int main(int argc, char *argv[]) {

    (void)argc;
    (void)argv;

//////////////////////////////////////////////////////////////////////////////////////////////
// CREATE DECENTRALIZED UTT SYSTEM
//////////////////////////////////////////////////////////////////////////////////////////////

    Simulation::initialize();

    size_t n = 21;
    size_t thresh = 12;

    Simulation::Context ctx = Simulation::createContext(n, thresh);
    
    logdbg << "Created decentralized UTT system" << endl;

//////////////////////////////////////////////////////////////////////////////////////////////
// CREATE ACCOUNT WALLETS
//////////////////////////////////////////////////////////////////////////////////////////////

    // Creates users and their wallets
    size_t numWallets = 3;
    // Each wallet is initialized with the following normal coins
    std::vector<size_t> normal_coin_values = { 100, 100 };
    size_t budget = 10000; // Only one budget coin can be stored in the wallet currently
          
    std::vector<Wallet> w;
    for (size_t i = 0; i < numWallets; ++i)
        w.emplace_back(Simulation::createWallet(ctx, "user_" + std::to_string(i), normal_coin_values, budget));


    logdbg << "Created random wallets" << endl;

    // nullifer list
    std::set<std::string> nullset;

//////////////////////////////////////////////////////////////////////////////////////////////
// POSSIBLE TRANSACTIONS
//////////////////////////////////////////////////////////////////////////////////////////////

    using TxFnType = std::function<void(Wallet&, Wallet&)>;
    std::map<std::string, int> tx_fn_counter; // Keep track of the simulated distribution

    // Payment of two coins with change
    auto payment_2t2 = [&](Wallet& w1, Wallet& w2){
        Simulation::doRandomPayment_2t2(ctx, w1, w2, nullset, w);
        tx_fn_counter["payment_2t2"] += 1;
    };

    // Payment of two coins with change
    auto payment_2t1 = [&](Wallet& w1, Wallet& w2){
        Simulation::doRandomPayment_2t1(ctx, w1, w2, nullset, w);
        tx_fn_counter["payment_2t1"] += 1;
    };

    // Payment of one coin with change
    auto payment_1t2 = [&](Wallet& w1, Wallet& w2){
        Simulation::doRandomPayment_1t2(ctx, w1, w2, nullset, w);
        tx_fn_counter["payment_1t2"] += 1;
    };

    // Exact payment of one coin
    auto payment_1t1 = [&](Wallet& w1, Wallet& w2){
        Simulation::doRandomPayment_1t1(ctx, w1, w2, nullset, w);
        tx_fn_counter["payment_1t1"] += 1;
    };

    // Coin split
    auto self_1t2 = [&](Wallet& w1, Wallet&){
        Simulation::doRandomCoinSplit(ctx, w1, nullset, w);
        tx_fn_counter["self_1t2"] += 1;
    };
    
    // Coin merge
    auto self_2t1 = [&](Wallet& w1, Wallet&){
        Simulation::doRandomCoinMerge(ctx, w1, nullset, w);
        tx_fn_counter["self_2t1"] += 1;
    };

//////////////////////////////////////////////////////////////////////////////////////////////
// SIMULATE
//////////////////////////////////////////////////////////////////////////////////////////////

    int total_fn = 0;
    size_t numCycles = 20;
    for(size_t cycle = 0; cycle < numCycles; cycle++) {
        logdbg << endl << endl;
        logdbg << "================ Cycle #" << (cycle + 1) << " ================" << endl;

        // Pick next possible transaction at random
        // Use wallets in a round-robin fashion
        // Try each wallet once per cycle and stop early if none can produce transactions
        
        TxFnType fn;
        Wallet* w1 = nullptr;
        Wallet* w2 = nullptr;

        for (size_t i = 0; i < w.size(); ++i) {
            size_t w1_i = (cycle + i) % w.size();
            w1 = &w[w1_i];

            std::vector<TxFnType> fn_pool;
            if (!w1->coins.empty()) {
                // Assume that we always have enough budget

                fn_pool.push_back(payment_1t1);

                // Requires a coin to be split
                for (const auto& c : w1->coins) {
                    if (c.val.as_ulong() > 1) {
                        fn_pool.push_back(payment_1t2); 
                        fn_pool.push_back(self_1t2);
                        break;
                    }
                }

                if (w1->coins.size() > 1) {
                    fn_pool.push_back(payment_2t1);
                    fn_pool.push_back(payment_2t2);
                    fn_pool.push_back(self_2t1);
                }
            }

            if (fn_pool.empty()) continue; // try next wallet
            fn = fn_pool[static_cast<size_t>(rand()) % fn_pool.size()];

            // Always pick a second wallet at random (if we do a self transaction it won't change)
            size_t rand_offset_from_i = 1 + (static_cast<size_t>(rand()) % (w.size() - 1));
            size_t w2_i = (w1_i + rand_offset_from_i) % w.size();
            assertNotEqual(w1_i, w2_i);

            w2 = &w[w2_i];

            break; // Done
        }

        // There should always be at least one possible payment, split or merge among all wallets
        assertNotNull(fn); 
        assertNotNull(w1);
        assertNotNull(w2);

        Simulation::assertSerialization(*w1);
        Simulation::assertSerialization(*w2);

        fn(*w1, *w2);
        total_fn += 1;

    } // end for all cycles

    // Check total value in the system
    size_t wallet_value = 0;
    for (const auto& v : normal_coin_values) wallet_value += v;
    size_t total_value_expected = w.size() * wallet_value;

    size_t total_value_actual = 0;
    for (const auto& wallet : w) {
        for (const auto& c : wallet.coins) {
            total_value_actual += c.getValue();
        }
    }
    testAssertEqual(total_value_expected, total_value_actual);

    logdbg << "Simulated Tx distribution [total=" << total_fn << "]:" << endl;
    if (total_fn > 0) {
        for (const auto& kvp : tx_fn_counter)
            logdbg << kvp.first << " : " << std::setprecision(3) << ((float)kvp.second / (float)total_fn) << endl;
    }

    loginfo << "All is well." << endl;

    return 0;
}
