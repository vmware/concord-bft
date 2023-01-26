// UTT Client API
//
// Copyright (c) 2020-2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#include <iostream>
#include <string>
#include <sstream>

#include "wallet.hpp"

// [TODO-UTT] The wallet should use RocksDB to store UTT assets.
// [TODO-UTT] The wallet should use some RSA cryptography to generate public/private
// keys (used by the UTT Client Library)

// [TODO-UTT] Initial registration
// Upon first launch (no records in RocksDB) the wallet asks the user to register
// > Choose a unique user identifier:
// After this prompt the wallet sends a registration request and waits for the response
// Upon successful registration the user can use any of the following commands.

// [TODO-UTT] Startup Sequence
// A. Confirm registration -- check that the user is registered, otherwise go to "Initial registration".
// B. Synchronize
// 1) Ask about the latest signed transaction number and compare with
// the latest executed transaction in the wallet. Determine the range of tx numbers to be retrieved.
// 2) For each tx number to execute request the transaction and signature (can be combined)
// 3) Apply the transaction to the wallet state
//  a. If it's a burn or a mint transaction matching our user-id
//  b. IF it's an anonymous tx that we can claim outputs from or slash spent coins (check the nullifiers)
// [TODO-UTT] Synchronization can be optimized to require fewer requests by batching tx requests and/or filtering by
// user-id for burns and mints

// [TODO-UTT] Periodic synchronization
// We need to periodically sync with the wallet service - we can either detect this when we send requests
// (we see that there are multiple transactions that happened before ours) or we do it periodically or before
// attempt an operation.

// Note: Limited recovery from liveness issues
// In a single machine demo setting liveness issues will not be created due to the network,
// so we don't need to implement the full range of precautions to handle liveness issues
// such as timeouts.

void printHelp() {
  std::cout << "\nCommands:\n";
  std::cout << "config                    -- configures wallets with the privacy application.\n";
  std::cout << "show                      -- prints information about the user managed by this wallet.\n";
  std::cout << "register <user-id>        -- requests user registration required for spending coins.\n";
  std::cout << "convertPublicToPrivate <amount>             -- converts the specified amount of public funds to "
               "private funds.\n";
  std::cout << "transfer <amount> <to-user-id> -- transfers the specified amount between users.\n";
  std::cout
      << "public-transfer <amount> <to-user-id> -- transfers the specified amount of public funds between users.\n";
  std::cout << "convertPrivateToPublic <amount>             -- converts the specified amount of private funds to "
               "public funds.\n";
  std::cout << '\n';
}

struct CLIApp {
  // For this demo we assume all the private and public key are pre known
  const std::map<std::string, std::pair<std::string, std::string>> k_TestKeys{
      {"user-1",
       {"-----BEGIN PRIVATE KEY-----\n"
        "MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDRMymAGtHdMtc2\n"
        "J6pDnk0fSt9ZUDT62yXkNF+jBY4zxLuBMq/qhp+nXaa2rB2haqtJxMpAQH/xy5D8\n"
        "l7smiMjgxsLre9oeV5dH9hjVu+HzqulEbFNGtWOBqQg8mFO6orwxLoZXsyUeumF8\n"
        "wFFfY0QDZ9IZDZPP7gdXy8iKFe2z4A3KKiG1Xgp+DQWU2qM1AppekluNWYXRVyZg\n"
        "buHNmTMdbZTfat5rzPfNOPSTRPJTSsbKSufidxe1iS+QS1OW4UzKfDx8r2Z0AgDJ\n"
        "0CWvOmArMM5YZjkr+4C4iurhHd20pnAb29QuYWqOcUIIfR31QM3zSwi3qmCs2ECw\n"
        "d28tN4zfAgMBAAECggEAf3xQNBMkBUqwP/5YEjDsCr/T8FeikaTvKGyKQ4xlJkMj\n"
        "iQ5cie1Uaef2aqfkvrOEgsX8Ar/LuIw3ZNcKY+dDk0dNDbhCKe9y75WXeNiwT4+9\n"
        "68af0R1E8IUT0el6TOhTCx2xHMy9OEaYli+U5y5VCkZAkKfnhfUmYGh9YzkNzGkJ\n"
        "dhGepKcmSfV68qzwptT8KqFfyBVRDNsVPT2FQ228DaJGYiMSz1H72KTOvq8oNtLa\n"
        "5itjY9By3PUrM3Zl7I/0uSmbYkq1izKlN9aoVVA8D5otSnoArsbjRjNzT1NsMEDj\n"
        "SO41ADjmrPQFoYfK6neKbWJ637XsWJNbF11SOJ74oQKBgQDw2RGHtOpPzAHR7kcN\n"
        "6I1ycDrxliwHoiGkt/qiRbvbvZtvg3nM1QDruyrT4MV8IaEGth6qxw/LDZl/PKi/\n"
        "dUV9S+r0gV7Z2Eja1j8oETZsEGgfsqpgtxgqjee0c4n4ijks5i5ByIam6ezPk0CX\n"
        "drPHmKTa3wv25yvsqUDqA4yneQKBgQDeXGQ4rzN4VS2XdxeoZtfgGo9lSi3ooG7p\n"
        "yrDr3/6tIWYZln5WbeI+Ym58XNLFs/UYLuaAXX2xAD1GC8GyRwfV2IxNtnvyELqe\n"
        "r5VZLUyr/nAKNy4DCgmDirsMQi2BiCIW+3dbaSMWk0hyRL4XywgDT1LIv1iBjME9\n"
        "VRdOcqtJFwKBgQCOnJnqt09/DJePTP452BfZSWc9oeRUaMZvGJmJ+KyyAuXE9B7t\n"
        "ELtI2j0T1KZbSDZnGPOzv8c0PstDMhHhvHj3JjrrqKcXayBIpFAlU3vcJSLikhKc\n"
        "zg27NOecTEXIK2CLm4iMX0aMEzur9c9rYFg3ucTz7NrSyjOSIO5VIuDoUQKBgGq/\n"
        "ZAWqZkUwbxC1xY+8v4oAdjKkJ+Hzkwt9mO3DvNmUnRVPoBsR6XkVfAEL9suelt0j\n"
        "NtkNCNg+SywjXLufSe2pZcGxB1OwIEcp98K18obnQRZGYzpmSSbzJNS/uGTk26i4\n"
        "1BX4JTYjQrZIthFqENC19gIVigG3dtVg0i2A8yXFAoGBALJAAyZe0CviVpR7uBeS\n"
        "fhvN1+0T4mVo1p00Cn5I9eUPMaHkuFoXSgy96PVvfbI+s5vZOEqqrkr3ERfl9Lo7\n"
        "FXEtnQuUZ+zrURHHYxwzYj10MFYaUV408ywQ0KpKqpA1KJpnxLZEidFbZSKUliOa\n"
        "1BNh6wL6XelbX7hpe53RpXSU\n"
        "-----END PRIVATE KEY-----",
        "-----BEGIN PUBLIC KEY-----\n"
        "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA0TMpgBrR3TLXNieqQ55N\n"
        "H0rfWVA0+tsl5DRfowWOM8S7gTKv6oafp12mtqwdoWqrScTKQEB/8cuQ/Je7JojI\n"
        "4MbC63vaHleXR/YY1bvh86rpRGxTRrVjgakIPJhTuqK8MS6GV7MlHrphfMBRX2NE\n"
        "A2fSGQ2Tz+4HV8vIihXts+ANyiohtV4Kfg0FlNqjNQKaXpJbjVmF0VcmYG7hzZkz\n"
        "HW2U32rea8z3zTj0k0TyU0rGykrn4ncXtYkvkEtTluFMynw8fK9mdAIAydAlrzpg\n"
        "KzDOWGY5K/uAuIrq4R3dtKZwG9vULmFqjnFCCH0d9UDN80sIt6pgrNhAsHdvLTeM\n"
        "3wIDAQAB\n"
        "-----END PUBLIC KEY-----"}},
      {"user-2",
       {"-----BEGIN PRIVATE KEY-----\n"
        "MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQDmtzssQWb6iagv\n"
        "gmn8WGqeyvl1jVPEjtuD+OC/i29NByi/SzuEBHmnEMbThXyy2hE9wbxL9fcwv9P7\n"
        "DGhOiHPfWryUksoiJtkAxhJeAofcPH8h40cn35CZC/z52MBf6jxhlsr8YzaPXALO\n"
        "KWuVxHXWm4XRQuYasHc17FlC+QOB/piwT1pJzJQL82XMVPQ3BiiPVBYmBIIGZ4UV\n"
        "Elbv0G8oiuX8sspJxK5u0QlKbXe4WaCpwn5+9eW32xMd5IwMGKsz2GMU+TWovJk6\n"
        "V4QairTtv7KYNo8noGVEPP8WiieHc6K/esqaqOMEOjSjbHIkz8x4x2a3EcQ8ZqqE\n"
        "5ECJCHyNAgMBAAECggEAAj4TsCluk87UuKl+2DoPxz1X0WGvR8DPt2eq35G+YjRI\n"
        "nBtiF7VnbU1HcXThsmdWNZB1gROB9JJYwB7twJ2o6qnaHWOT3WMdRtmmAg3qPiTE\n"
        "Y5Lu9R9CK4qnSdJmhEglkBlEHdHlDN2rFU/nwMvpDU3zrgYFcIr4jaifcXw0IYZQ\n"
        "QcohiItC72Nz9/xYVOYkzzj4CbltAvH7QEniVPEkiHuKcMFocb0Qr1xUdw7cYCpH\n"
        "wgLoUk+7mv7QNo1gDVtIfFJcig1w/YMe6wLZV0cs2jCzQqJL/H24cDnSobqFld+F\n"
        "W0HFazNX8dw50213oSlINHH7Ys9abPV7UXeolHWTbQKBgQD5MJnGt6HZPTPu6aWc\n"
        "HN9EVR0iYU5J4lxYFdvFnTC1uWUxwQGa9h3ohvt7J7RgOe1zfoDHQBzjTmTz2emt\n"
        "Z3K4xVVxwatyZO460AAWvJDJRpHx6QMBpVS1rek0LifaR8sac4NIX3Km7UcmWywR\n"
        "Byt5T5gHL8otwvmVn4BmWIn+IwKBgQDtBWFz8+g58Y5KgH/LWEC6r6df/5oxj8BY\n"
        "sZhgw8Oz9VjGMuleSc0vByzVg9kRdIOEQrNQrVgJwU5AeZiRL/eXw8W2FQUBLHPF\n"
        "pYMXUBrSluGdlg+JLGkNP2pa9mNhlgVGeiHWPINWiDZL6OMejWawge0hUe8UI3XP\n"
        "yuAD8CFNjwKBgF+NHUovm+YbK9DO0uwbvhkLshI+0bBPFi3Io+8QqV7lakI7ygvL\n"
        "mAhTyhadUPMdA0ooFeVRVkJrCxbeVlZhtoHXWT43jzBcN2Vh4MbXI4Wqg8gJG1gE\n"
        "N4k62JNjp3Bx2xWXeZ3Ey8fqcD/q8ejNoQPxW1BWKOaHPwD6mlekV7WVAoGAJ8+/\n"
        "jrppR1JLlDgBPyaTuMfIVVeZjrmWlU8/SQGY3aFYR/JFQJEk5cFOxo5e54+qh4ys\n"
        "keCL6RePDUVfWwOzkspPa0YckRXmXExwuHm8B6NQQifydBgBjTgZpS29g6avPCdH\n"
        "h7SiWuaGODnl7DvUA9HPAsnAXqGWKFDrT74F2gECgYB8s752mNPfKerMuTH7cRmZ\n"
        "cX1GdwshZVnA745mlEgwNgbeF3ir9ZGPsK5j0GuYgy1+pwjp8eT4OT8gWcJJjU7t\n"
        "9i2pQzrqCArD4WMQ+XhADM26fNL3utI+kEhK205KHy42yE0lCCqoTtPxdwZAru3Q\n"
        "5apYHTHKXTFGxzEZBGn3lg==\n"
        "-----END PRIVATE KEY-----",
        "-----BEGIN PUBLIC KEY-----\n"
        "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA5rc7LEFm+omoL4Jp/Fhq\n"
        "nsr5dY1TxI7bg/jgv4tvTQcov0s7hAR5pxDG04V8stoRPcG8S/X3ML/T+wxoTohz\n"
        "31q8lJLKIibZAMYSXgKH3Dx/IeNHJ9+QmQv8+djAX+o8YZbK/GM2j1wCzilrlcR1\n"
        "1puF0ULmGrB3NexZQvkDgf6YsE9aScyUC/NlzFT0NwYoj1QWJgSCBmeFFRJW79Bv\n"
        "KIrl/LLKScSubtEJSm13uFmgqcJ+fvXlt9sTHeSMDBirM9hjFPk1qLyZOleEGoq0\n"
        "7b+ymDaPJ6BlRDz/Foonh3Oiv3rKmqjjBDo0o2xyJM/MeMdmtxHEPGaqhORAiQh8\n"
        "jQIDAQAB\n"
        "-----END PUBLIC KEY-----"}},
      {"user-3",
       {"-----BEGIN PRIVATE KEY-----\n"
        "MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDKI22rxWJQWA0M\n"
        "zt0QA91dv6u9ZRXN97ojkEU3byqf6VPhpzoFrEUo0pUj/8kApCoBphJnlqUc2qJT\n"
        "t8rtonD8SscVAboTHLHHwC4RWR4hVMml+GWPdrucpIdA8vGvnAoSLtcZltyrdyGO\n"
        "OM4G7TOI+At6wWRZSJq9+3F748It5g5tWeDPZQ/HsS8h5rYHgItvQOM/QV43XJRA\n"
        "X0b9S2S8zAfwcg8Veb8xxGr9+OCSGYmUMnztT+6cTHcWrmf04iR4Xe6sdvp8/3JZ\n"
        "hG0aDHXnAhv6hrqDbR1/qe+wA8wWklT/5i2YKhQy/zAsZP6+SR6uXcb4RmFezFCn\n"
        "1QPHahdHAgMBAAECggEBAJ10n3dQF9cR34h8kwJooEToivTLCwMX1yzgsqoNtBxA\n"
        "epCp3K6SlITKFFaoZoBTYE1MecWrOQ6S2CNyZWZaRLyh2xXn9HPyxCEe+EOXMbsw\n"
        "+qqIJURtbpMELj9PfygV2lpzliZet6Hw+Hh6kzIeDyjAXuDOSEasIa2fcbTZo2Zw\n"
        "DUQNimJ8zm2vqzTXaDQxUk8vWTvYjaTU7HpjKK0Y6VL/MSb36a8db3iCdtWMhfQr\n"
        "/sEDIc9PXwE2dXEs7quz79PjrX9WyAW+lI0B4jw6s4O+3jgSwZ5/n1pE3Q1QDLic\n"
        "DspyOkWI9bAeQkANQNXZsJtBpBWAwfVEcD6K+mNHGWECgYEA7iS9fxiUSjWJeKDS\n"
        "vHPvqIUB72KinnDt5WnpN+ac/uvg8tUUal9Ka2VpfOvt7DUnfu/8fxMOW5sEwWrD\n"
        "c34Nx7aweKDzNNBX13IV30rmrQWfXKIHF0zl2iCe1n4ZbRxtJ+XzS5eI56ofKb46\n"
        "bVy8mWIffF8VT8tX4Ps01mmUXQ8CgYEA2UuOMq0Z4aFzDybAFXrtGBF2ZydR7TYY\n"
        "vqCgERK4wYZ1Its7ikyUc178L4g+BtLcdraJAC1ai7sTvn/OgytJAaP2hzrxOdy4\n"
        "9ufXQ7YmUUmt6vgC0svPPBm6rRuBHMo4jlLcQ1GZxCIniGO32OzVl61L4nABviGj\n"
        "r+XG6oJnkkkCgYAom10wmdlXWg+p4QpuFfrMqnls/02pZKo8DzY0UP9+PGxsG8dS\n"
        "aBNBgtZc9cHpIBsu+u78IBy3pBRIgtL3E4x3/H/U4eT2oXwJAawXGieBY08MNZit\n"
        "8W/UJPHQs0nUoB9AyWYWAq3WityreoNt+H00TlX+GByDMbI3a75TvUcOIwKBgQCF\n"
        "LnHxTXdXe4Tx/GYuxDEdV1ai0s67/TQdYoW48SicNLUGsChGE7nJHKak23Ro6kSF\n"
        "3ksJ+MJOclMfp2YDrzwH7V7kc5P2SvpzGpYtwi6qE+as5WWVnaVeyMZJ41m3M5qG\n"
        "YPLvr8v3Epf4WGYOZtpjJKwmFVB8IFIbNVI92b9xQQKBgE77nz2wIPVM6Diwe1ro\n"
        "jgRURprOPDj5eT9JkMgyH8Qe9iKI5qCk8ps/s+P1hrZnjNkerdYilMuid20V9Lib\n"
        "/MKAhAV4UCIIVSMzP4bHpP+ZLhVHee4InLLA9EShYJGTdL1V5YYbeTTWiym4TetH\n"
        "oeRZA5cnrpr7PMvfRP0+sx7W\n"
        "-----END PRIVATE KEY-----",
        "-----BEGIN PUBLIC KEY-----\n"
        "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAyiNtq8ViUFgNDM7dEAPd\n"
        "Xb+rvWUVzfe6I5BFN28qn+lT4ac6BaxFKNKVI//JAKQqAaYSZ5alHNqiU7fK7aJw\n"
        "/ErHFQG6Exyxx8AuEVkeIVTJpfhlj3a7nKSHQPLxr5wKEi7XGZbcq3chjjjOBu0z\n"
        "iPgLesFkWUiavftxe+PCLeYObVngz2UPx7EvIea2B4CLb0DjP0FeN1yUQF9G/Utk\n"
        "vMwH8HIPFXm/McRq/fjgkhmJlDJ87U/unEx3Fq5n9OIkeF3urHb6fP9yWYRtGgx1\n"
        "5wIb+oa6g20df6nvsAPMFpJU/+YtmCoUMv8wLGT+vkkerl3G+EZhXsxQp9UDx2oX\n"
        "RwIDAQAB\n"
        "-----END PUBLIC KEY-----"}}};

  std::string userId;
  grpc::ClientContext ctx;
  Wallet::Connection conn;
  Wallet::Channel chan;
  utt::Configuration config;
  std::unique_ptr<Wallet> wallet;

  CLIApp() {
    conn = Wallet::newConnection();
    if (!conn) throw std::runtime_error("Failed to create wallet connection!");

    chan = conn->walletChannel(&ctx);
    if (!chan) throw std::runtime_error("Failed to create wallet streaming channel!");
  }

  ~CLIApp() {
    std::cout << "Closing wallet streaming channel... ";
    chan->WritesDone();
    auto status = chan->Finish();
    std::cout << " Done.\n";
    // std::cout << "gRPC error code: " << status.error_code() << '\n';
    // std::cout << "gRPC error msg: " << status.error_message() << '\n';
    // std::cout << "gRPC error details: " << status.error_details() << '\n';
  }

  void configure() {
    if (wallet) {
      std::cout << "The wallet is already configured.\n";
      return;
    }

    auto publicConfig = Wallet::getPublicConfig(chan);

    wallet = std::make_unique<Wallet>(userId, k_TestKeys.at(userId).first, k_TestKeys.at(userId).second, publicConfig);
  }

  void registerUserCmd() {
    if (!wallet->isRegistered()) {
      wallet->registerUser(chan);
      wallet->showInfo(chan);
    } else {
      std::cout << "Wallet is already registered.\n";
    }
  }

  void showCmd() { wallet->showInfo(chan); }

  void mintCmd(const std::vector<std::string>& cmdTokens) {
    if (cmdTokens.size() != 2) {
      std::cout << "Usage: convertPublicToPrivate <amount>\n";
      return;
    }
    int amount = std::atoi(cmdTokens[1].c_str());
    if (amount <= 0) {
      std::cout << "Expected a positive public funds amount!\n";
      return;
    }
    wallet->mint(chan, (uint64_t)amount);
    wallet->showInfo(chan);
  }

  void transferCmd(const std::vector<std::string>& cmdTokens) {
    if (cmdTokens.size() != 3) {
      std::cout << "Usage: transfer <amount> <to-user-id>\n";
      return;
    }
    int amount = std::atoi(cmdTokens[1].c_str());
    if (amount <= 0) {
      std::cout << "Expected a positive transfer amount!\n";
      return;
    }
    const auto& userId = cmdTokens[2];
    wallet->transfer(chan, (uint64_t)amount, userId, k_TestKeys.at(userId).second);
    wallet->showInfo(chan);
  }

  void publicTransferCmd(const std::vector<std::string>& cmdTokens) {
    if (cmdTokens.size() != 3) {
      std::cout << "Usage: transfer <amount> <recipient>\n";
      return;
    }
    int amount = std::atoi(cmdTokens[1].c_str());
    if (amount <= 0) {
      std::cout << "Expected a positive transfer amount!\n";
      return;
    }
    wallet->publicTransfer(chan, (uint64_t)amount, cmdTokens[2]);
    wallet->showInfo(chan);
  }

  void burnCmd(const std::vector<std::string>& cmdTokens) {
    if (cmdTokens.size() != 2) {
      std::cout << "Usage: convertPrivateToPublic <amount>\n";
      return;
    }
    int amount = std::atoi(cmdTokens[1].c_str());
    if (amount <= 0) {
      std::cout << "Expected a positive private funds amount!\n";
      return;
    }
    wallet->burn(chan, (uint64_t)amount);
    wallet->showInfo(chan);
  }

  void debugCmd() { wallet->debugOutput(); }
};

int main(int argc, char* argv[]) {
  (void)argc;
  (void)argv;

  if (argc != 2) {
    std::cout << "Provide a user-id.\n";
    return 0;
  }

  CLIApp app;
  app.userId = argv[1];

  if (app.k_TestKeys.find(app.userId) == app.k_TestKeys.end()) {
    std::cout << "Selected user id has no pre-generated keys!\n";
    std::cout << "Valid user ids: [";
    for (const auto& [userId, _] : app.k_TestKeys) std::cout << userId << ", ";
    std::cout << "]\n";
    return 0;
  }

  std::cout << "\nSample Privacy Wallet CLI Application.\n";
  std::cout << "UserId: " << app.userId << '\n';

  try {
    utt::client::Initialize();

    while (true) {
      std::cout << "\nEnter command (type 'h' for commands 'Ctr-D' to quit):\n > ";
      std::string cmd;
      std::getline(std::cin, cmd);

      if (std::cin.eof()) {
        std::cout << "Quitting...\n";
        break;
      }

      if (cmd == "h") {
        printHelp();
      } else if (cmd == "config") {
        app.configure();
      } else if (!app.wallet) {
        std::cout << "You must first configure the wallet. Use the 'config' command.\n";
      } else {
        // Tokenize params
        std::vector<std::string> cmdTokens;
        std::string token;
        std::stringstream ss(cmd);
        while (std::getline(ss, token, ' ')) cmdTokens.emplace_back(token);
        if (cmdTokens.empty()) continue;

        if (cmdTokens[0] == "register") {
          app.registerUserCmd();
        } else if (!app.wallet->isRegistered()) {
          std::cout << "You must first register the user. Use the 'register' command.\n";
        } else if (cmdTokens[0] == "show") {
          app.showCmd();
        } else if (cmdTokens[0] == "convertPublicToPrivate") {
          app.mintCmd(cmdTokens);
        } else if (cmdTokens[0] == "transfer") {
          app.transferCmd(cmdTokens);
        } else if (cmdTokens[0] == "public-transfer") {
          app.publicTransferCmd(cmdTokens);
        } else if (cmdTokens[0] == "convertPrivateToPublic") {
          app.burnCmd(cmdTokens);
        } else if (cmdTokens[0] == "debug") {
          app.debugCmd();
        } else {
          std::cout << "Unknown command '" << cmd << "'\n";
        }
      }
    }
  } catch (const std::runtime_error& e) {
    std::cout << "Error (exception): " << e.what() << '\n';
    return 1;
  }

  return 0;
}