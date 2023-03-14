

/**
 * Checks that the std::optional's are all set and thus this coin
 * is ready to be spent.
 *
 * WARNING: Anticipating use for testing only.
 */
bool isReadyToSpend() const {
  return ck.has_value() && ccm_txn.has_value() && sig.has_value() && null.has_value() &&
         (isNormal() || exp_date.has_value()) && vcm.has_value() && z.has_value() && true;
}
