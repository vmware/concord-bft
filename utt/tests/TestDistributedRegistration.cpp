#include "details.hpp"
#include "testUtils.hpp"

#include <memory>
#include <vector>
#include <cstdlib>
#include <ctime>

using namespace libutt;
using namespace libutt::api;

int main(int argc, char* argv[]) {
  (void)argc;
  (void)argv;
  size_t thresh = 3;
  size_t n = 4;
  size_t c = 10;
  auto [dkg, rc] = testing::init(n, thresh);
  auto& d = Details::instance();
  auto registrators = testing::GenerateRegistrators(n, rc);
  auto banks = testing::GenerateCommitters(n, dkg);
  auto clients = testing::GenerateClients(c, dkg.getSK().toPK());

  for (auto& c : clients) {
    testing::registerClient(d, c, registrators, thresh);
    for (auto& r : registrators) {
      auto rcm_data = c.getRcm();
      assertTrue(r->validateRCM(rcm_data.first, rcm_data.second));
    }
  }

  return 0;
}