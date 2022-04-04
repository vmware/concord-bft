
#include "threshsign/ThresholdAccumulatorBase.h"

#include <utility>
#include <cstring>

#include "Utils.h"
#include "Logger.hpp"
#include "XAssert.h"

using std::endl;
using namespace threshsign;

template <class VerificationKey, class NumType, typename SigShareParserFunc>
int ThresholdAccumulatorBase<VerificationKey, NumType, SigShareParserFunc>::add(const char* sigShare, int len) {
  std::pair<ShareID, NumType> parsed = SigShareParserFunc()(sigShare, len);

  return addNumById(parsed.first, parsed.second);
}

template <class VerificationKey, class NumType, typename SigShareParserFunc>
void ThresholdAccumulatorBase<VerificationKey, NumType, SigShareParserFunc>::setExpectedDigest(const unsigned char* msg,
                                                                                               int len) {
  assertNotNull(msg);
  assertStrictlyPositive(len);

  if (!hasExpectedDigest()) {
    expectedDigest.reset(new unsigned char[static_cast<size_t>(len)]);
    memcpy(reinterpret_cast<void*>(expectedDigest.get()),
           reinterpret_cast<const void*>(msg),
           static_cast<unsigned long>(len));
    expectedDigestLen = len;
    assertTrue(hasExpectedDigest());

    onExpectedDigestSet();
    // If share verification is enabled, move pending shares to valid shares after verifying them
    if (hasShareVerificationEnabled()) {
      verifyPendingShares();
    } else {
      LOG_TRACE(THRESHSIGN_LOG, "Share verification is disabled, nothing to move.");
    }
  } else {
    // NOTE: As discussed with Guy, we don't allow callers to change the expected digest.

    if (expectedDigestLen != len) {
      LOG_ERROR(THRESHSIGN_LOG,
                "Attempted to reset expected digest of different length. "
                    << "Previously had " << expectedDigestLen << ", given " << len);
      throw std::runtime_error("Cannot reset expected digest with different length");
    }

    if (memcmp(expectedDigest.get(), msg, static_cast<size_t>(len)) != 0) {
      LOG_ERROR(THRESHSIGN_LOG,
                "Attempted to reset expected digest to a different one. "
                    << "Previously had '" << Utils::bin2hex(expectedDigest.get(), len) << "', you gave '"
                    << Utils::bin2hex(msg, len) << "'");
      throw std::runtime_error("Cannot reset expected digest to a different one");
    }
  }
}

template <class VerificationKey, class NumType, typename SigShareParserFunc>
void ThresholdAccumulatorBase<VerificationKey, NumType, SigShareParserFunc>::verifyPendingShares() {
  assertTrue(hasShareVerificationEnabled());
  assertTrue(hasExpectedDigest());
  assertEqual(validSharesBits.count(), 0);

  for (ShareID id = pendingSharesBits.first(); pendingSharesBits.isEnd(id) == false; id = pendingSharesBits.next(id)) {
    // We have to stop if we reach required threshold number of signers
    if (validSharesBits.count() == reqSigners) break;

    // Get the share pending for this signer
    size_t idx = static_cast<size_t>(id);
    NumType& sigShare = pendingShares[idx];

    // Is it a valid share? If so mark it as valid.
    if (verifyShare(id, sigShare)) {
      validShares[idx] = sigShare;
      validSharesBits.add(id);  // If already added, not a problem.
      LOG_TRACE(THRESHSIGN_LOG, "Moved validated share by signer " << id);
    } else {
      invalidShares.insert(id);
      LOG_WARN(THRESHSIGN_LOG, "Invalid share by signer " << id << " detected: " << sigShare);
    }
  }
  pendingShares.clear();

  // We should not have accumulated more shares than exactly what we need
  assertLessThanOrEqual(validSharesBits.count(), reqSigners);
}

template <class VerificationKey, class NumType, typename SigShareParserFunc>
int ThresholdAccumulatorBase<VerificationKey, NumType, SigShareParserFunc>::addNumById(ShareID signer,
                                                                                       const NumType& sigShare) {
  assertInclusiveRange(1, signer, totalSigners);
  assertLessThanOrEqual(validSharesBits.count(), reqSigners);

  size_t idx = static_cast<size_t>(signer);

  // If not verifying shares or (verifying and no digest set yet), then mark the share as pending.
  //
  // NOTE: Here we accumulate shares from as many signers as we can, even more than reqSigners
  // because some of those shares might be invalid.
  if (hasShareVerificationEnabled() && !hasExpectedDigest()) {
    if (!pendingSharesBits.contains(signer)) {
      pendingShares[idx] = sigShare;
      pendingSharesBits.add(signer);  // If already added, not a problem.
      LOG_TRACE(THRESHSIGN_LOG, "added pending share for signer: " << signer);
    } else {
      LOG_WARN(THRESHSIGN_LOG, "Did NOT add extra pending share for signer: " << signer << " (NO digest set yet)");
    }

    return pendingSharesBits.count();

    // ...else verify the share and if correct mark it as valid
  } else {
    // ...but we have to stop if we reach required threshold number of signers
    if (validSharesBits.count() == reqSigners) {
      LOG_WARN(THRESHSIGN_LOG, "Already accumulated all the required shares");
      return validSharesBits.count();
    }

    // ...and we can't have duplicate sig shares for the same signer
    if (!validSharesBits.contains(signer)) {
      if (!hasShareVerificationEnabled() || verifyShare(signer, sigShare)) {
        validShares[idx] = sigShare;
        validSharesBits.add(signer);  // If already added, not a problem.
        if (hasShareVerificationEnabled()) {
          LOG_TRACE(THRESHSIGN_LOG, "Added valid sigshare #" << idx << ": " << validShares[idx] << " (digest is set)");
        } else {
          LOG_TRACE(THRESHSIGN_LOG,
                    "Added sigshare #" << idx << ": " << validShares[idx] << " (share verification disabled)");
        }
        onNewSigShareAdded(signer, sigShare);
      } else {
        LOG_WARN(THRESHSIGN_LOG,
                 "Did NOT accumulate invalid share: " << sigShare << " (share verification is on AND digest is set)");
      }
    } else {
      LOG_WARN(THRESHSIGN_LOG,
               "Did NOT add valid share for signer " << signer
                                                     << " multiple times (share verification is off or digest is set)");
      // throw std::logic_error("You are accumulating the same signature share twice!");
    }

    return validSharesBits.count();
  }
}
