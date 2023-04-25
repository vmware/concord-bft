namespace concord::util::containers {
template <typename Container, typename Predicate>
void erase_if(Container& items, const Predicate& predicate) {
  for (auto iter = items.begin(); iter != items.end();) {
    if (predicate(*iter)) {
      iter = items.erase(iter);
    } else {
      ++iter;
    }
  }
}
}  // namespace concord::util::containers