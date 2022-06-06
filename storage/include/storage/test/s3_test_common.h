// Copyright 2019 VMware, all rights reserved
//
// Unit tests for ECS S3 object store replication
// IMPORTANT: the test assume that the S3 storage is up.

#include "gtest/gtest.h"
#include "s3/client.hpp"
#include "Logger.hpp"
#include "s3/config_parser.hpp"
#include "sliver.hpp"

#include <random>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <experimental/filesystem>
namespace fs = std::experimental::filesystem;

namespace concord::storage::s3::test {

using concordUtils::Sliver;

class IS3Server {
 public:
  virtual void start(const concord::storage::s3::StoreConfig &) = 0;
  virtual void stop() = 0;
  virtual ~IS3Server() = default;
};

class MinioS3Server : public IS3Server {
 public:
  MinioS3Server(const fs::path &base_dir) : data_dir{base_dir / "minio_data_dir"} {}

  void start(const concord::storage::s3::StoreConfig &config) override {
    if (!std::getenv("MINIO_BINARY_PATH")) {
      LOG_FATAL(GL, "MINIO_BINARY_PATH environment not set");
      ConcordAssert(false && "MINIO_BINARY_PATH environment not set");
    }

    binary_path = std::getenv("MINIO_BINARY_PATH");
    ;
    std::error_code ec;
    if (!fs::exists(binary_path, ec)) {
      LOG_FATAL(GL, binary_path << " doesn't exist: " << ec.message());
      ConcordAssert(false && "minio executable not found");
    }
    LOG_INFO(GL, "starting s3 server");
    if ((pid = fork()) == 0) {
      auto access = std::string("MINIO_ROOT_USER=" + config.accessKey);
      auto secret = std::string("MINIO_ROOT_PASSWORD=" + config.secretKey);
      putenv((char *)access.c_str());
      putenv((char *)secret.c_str());
      putenv((char *)"CI=on");
      int status = execl(binary_path.c_str(), "minio", "server", data_dir.c_str(), NULL);
      // returns only on failure
      LOG_FATAL(GL, "execl failed: " << strerror(errno));
      ASSERT_EQ(status, 0);
    } else {
      // give the server time to start
      using namespace std::chrono_literals;
      std::this_thread::sleep_for(5s);
    }
  }

  void stop() override {
    kill(pid, SIGTERM);
    int wstatus = 0;
    if (waitpid(pid, &wstatus, 0) != -1) {
      LOG_INFO(GL, "S3 server stopped");
    } else {
      LOG_WARN(GL, "failed to stop S3 server: " << strerror(errno));
    }
  }

 protected:
  fs::path data_dir;
  fs::path binary_path;
  int pid = 0;
};

/**
 * S3 test fixture
 */
class S3Test : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    logging::initLogger("logging.properties");
    createTestDir();
    readConfiguration();
    state.server.reset(new MinioS3Server(state.test_dir));
    state.server->start(state.config);
    state.client.reset(new concord::storage::s3::Client(state.config));
    state.client->init(false);
  }

  static void TearDownTestCase() {
    state.server->stop();
    fs::remove_all(state.test_dir);
  }
  void SetUp() override {}

  void TearDown() override {}

  static void readConfiguration() {
    fs::path config_file(state.test_dir / "s3_client_test_config.txt");
    std::ofstream ofs(config_file.c_str(), std::ios::out | std::ios::trunc);
    ofs << "# test configuration for S3-compatible storage\n"
        << "s3-bucket-name: blockchain\n"
        << "s3-access-key: concordbft\n"
        << "s3-protocol: HTTP\n"
        << "s3-url: 127.0.0.1:9000\n"
        << "s3-secret-key: concordbft\n";
    ofs.flush();
    state.config = concord::storage::s3::ConfigFileParser(config_file.c_str()).parse();
    LOG_INFO(GL, " s3 configuration: " << state.config);
  }

  static void createTestDir() {
    std::srand(std::time(nullptr));  // use current time as seed for random generator
    state.test_dir = fs::temp_directory_path() / (std::string("s3_client_test_") + std::to_string(std::rand()));
    std::error_code ec;
    if (!fs::create_directory(state.test_dir, ec)) {
      LOG_FATAL(GL, "failed to create test directory: " << state.test_dir << "reason: " << ec.message());
      ASSERT_TRUE(false);
    }
    LOG_INFO(GL, "test directory: " << state.test_dir);
  }
  struct shared_state {
    concord::storage::s3::StoreConfig config;
    fs::path test_dir;
    std::shared_ptr<concord::storage::s3::Client> client;
    std::unique_ptr<IS3Server> server;
  };
  static shared_state state;
};

S3Test::shared_state S3Test::state;
}  // namespace concord::storage::s3::test
