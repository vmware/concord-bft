// Concord
//
// Copyright (c) 2020-2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the
// LICENSE file.

#include <asio/bind_executor.hpp>
#include <regex>

#include <arpa/inet.h>
#include <chrono>
#include <optional>

#include "AsyncTlsConnection.h"
#include "TlsDiagnostics.h"
#include "TlsWriteQueue.h"
#include "secrets_manager_enc.h"
#include "secrets_manager_plain.h"

namespace bft::communication::tls {

void AsyncTlsConnection::startReading() {
  auto self = shared_from_this();
  asio::post(strand_, [this, self] { readMsgSizeHeader(); });
}

void AsyncTlsConnection::readMsgSizeHeader(std::optional<size_t> bytes_already_read) {
  LOG_DEBUG(logger_, KVLOG(peer_id_.value()));
  auto self = shared_from_this();
  const size_t offset = bytes_already_read ? bytes_already_read.value() : 0;
  const size_t bytes_remaining = MSG_HEADER_SIZE - offset;
  auto buf = asio::buffer(read_size_buf_.data() + offset, bytes_remaining);
  status_.msg_size_header_read_attempts++;

  auto start = std::chrono::steady_clock::now();
  socket_->async_read_some(
      buf,
      asio::bind_executor(
          strand_,
          [this, self, bytes_already_read, bytes_remaining, start](const auto& error_code, auto bytes_transferred) {
            if (disposed_) {
              return;
            }
            if (error_code) {
              if (error_code == asio::error::operation_aborted) {
                // The socket has already been cleaned up and any references are invalid. Just return.
                LOG_DEBUG(logger_, "Operation aborted: " << KVLOG(peer_id_.value(), disposed_));
                return;
              }
              // Remove the connection as it is no longer valid, and then close it, cancelling any ongoing operations.
              LOG_WARN(
                  logger_,
                  "Reading message size header failed for node " << peer_id_.value() << ": " << error_code.message());
              return dispose();
            }

            if (!bytes_already_read) {
              startReadTimer();
            }

            if (bytes_transferred != bytes_remaining) {
              LOG_DEBUG(logger_,
                        "Short read on messsage header occurred"
                            << KVLOG(peer_id_.value(), bytes_remaining, bytes_transferred));

              histograms_.async_read_header_partial->recordAtomic(durationInMicros(start));
              readMsgSizeHeader(MSG_HEADER_SIZE - (bytes_remaining - bytes_transferred));
            } else {
              // The message size header was read completely.
              if (getReadMsgSize() > config_.bufferLength) {
                LOG_WARN(logger_,
                         "Message Size: " << getReadMsgSize() << " exceeds maximum: " << config_.bufferLength
                                          << " for node " << peer_id_.value());
                return dispose();
              }
              if (!bytes_already_read) {
                histograms_.async_read_header_full->recordAtomic(durationInMicros(start));
              } else {
                histograms_.async_read_header_partial->recordAtomic(durationInMicros(start));
              }

              readMsg();
            }
          }));
}

void AsyncTlsConnection::readMsgSizeHeader() { readMsgSizeHeader(std::nullopt); }

void AsyncTlsConnection::readMsg() {
  auto msg_size = getReadMsgSize();
  LOG_DEBUG(logger_, KVLOG(peer_id_.value(), msg_size, (void*)read_msg_.data()));
  auto self = shared_from_this();
  status_.msg_reads++;
  auto start = std::chrono::steady_clock::now();
  async_read(
      *socket_,
      asio::buffer(read_msg_.data(), msg_size),
      asio::bind_executor(strand_, [this, self, start](const asio::error_code& error_code, auto bytes_transferred) {
        if (disposed_) {
          return;
        }
        if (error_code) {
          if (error_code == asio::error::operation_aborted) {
            LOG_DEBUG(logger_, "Operation aborted: " << KVLOG(peer_id_.value(), disposed_));
            // The socket has already been cleaned up and any references are invalid. Just return.
            return;
          }
          // Remove the connection as it is no longer valid, and then close it, cancelling any ongoing
          // operations.
          LOG_WARN(logger_,
                   "Reading message of size <<" << getReadMsgSize() << " failed for node " << peer_id_.value() << ": "
                                                << error_code.message());
          return dispose();
        }

        // The Read succeeded.
        histograms_.async_read_msg->recordAtomic(durationInMicros(start));
        LOG_DEBUG(logger_, "Cancelling read timer: " << KVLOG(peer_id_.value(), (void*)read_msg_.data()));
        read_timer_.cancel();
        histograms_.received_msg_size->recordAtomic(bytes_transferred);
        {
          concord::diagnostics::TimeRecorder<true> scoped_timer(*histograms_.read_enqueue_time);
          receiver_->onNewMessage(peer_id_.value(), read_msg_.data(), bytes_transferred);
        }
        readMsgSizeHeader();
      }));
}

void AsyncTlsConnection::startReadTimer() {
  LOG_DEBUG(logger_, KVLOG(peer_id_.value()));
  auto self = shared_from_this();
  read_timer_.expires_from_now(READ_TIMEOUT);
  status_.read_timer_started++;
  read_timer_.async_wait(asio::bind_executor(strand_, [this, self](const asio::error_code& ec) {
    if (ec == asio::error::operation_aborted || disposed_) {
      // The socket has already been cleaned up and any references are invalid. Just return.
      LOG_DEBUG(logger_, "Operation aborted: " << KVLOG(peer_id_.value(), disposed_));
      status_.read_timer_stopped++;
      return;
    }
    LOG_WARN(logger_, "Read timeout from node " << peer_id_.value() << ": " << ec.message());
    status_.read_timer_expired++;
    dispose();
  }));
}

void AsyncTlsConnection::startWriteTimer() {
  auto self = shared_from_this();
  write_timer_.expires_from_now(WRITE_TIMEOUT);
  status_.write_timer_started++;
  write_timer_.async_wait(asio::bind_executor(strand_, [this, self](const asio::error_code& ec) {
    if (ec == asio::error::operation_aborted || disposed_) {
      // The socket has already been cleaned up and any references are invalid. Just return.
      LOG_DEBUG(logger_, "Operation aborted: " << KVLOG(peer_id_.value(), disposed_));
      status_.write_timer_stopped++;
      return;
    }
    LOG_WARN(logger_, "Write timeout to node " << peer_id_.value() << ": " << ec.message());
    status_.write_timer_expired++;
    dispose();
  }));
}

uint32_t AsyncTlsConnection::getReadMsgSize() {
  LOG_DEBUG(logger_, KVLOG(peer_id_.value(), (void*)read_size_buf_.data()));
  // We send in network byte order
  // We must use memcpy to get aligned access
  uint32_t num;
  memcpy(&num, read_size_buf_.data(), 4);
  return ntohl(num);
}

void AsyncTlsConnection::remoteDispose() {
  auto self = shared_from_this();
  asio::post(strand_, [this, self] {
    static constexpr bool close_connection = false;
    dispose(close_connection);
  });
}

void AsyncTlsConnection::dispose(bool close_connection) {
  // We only want to dispose of a connection if it was actually authenticated, in which case it has
  // started to be used. In the case of a server connection failing authentication, it will have no
  // corresponding peer_id_.
  if (disposed_ || !peer_id_.has_value()) return;
  LOG_WARN(logger_, "Closing connection to node " << peer_id_.value());
  disposed_ = true;
  read_timer_.cancel();
  write_timer_.cancel();
  auto self = shared_from_this();
  if (close_connection) {
    // The ConnMgr runs in a separate strand. We must post a message to inform it, rather than calling directly.
    connection_manager_.remoteCloseConnection(peer_id_.value());
  }
}

void AsyncTlsConnection::send(std::shared_ptr<OutgoingMsg>&& msg) {
  concord::diagnostics::TimeRecorder<true> scoped_timer(*histograms_.send_post_to_conn);
  auto self = shared_from_this();
  asio::post(strand_, [this, self, msg{move(msg)}]() { write(msg); });
}

void AsyncTlsConnection::write(std::shared_ptr<OutgoingMsg> msg) {
  if (disposed_ || !msg) return;

  bool expected = true;
  // There is already an in-flight msg
  if (write_msg_used_.compare_exchange_weak(expected, true)) {
    write_queue_.push(std::move(msg));
    return;
  }
  expected = false;
  // Set the in-flight msg
  if (write_msg_used_.compare_exchange_weak(expected, true)) {
    write_msg_ = std::move(msg);
  } else {
    write_queue_.push(std::move(msg));
    LOG_ERROR(logger_, "write_msg_ already in use by other thread, msg pushed to the write queue.");
    return;
  }
  LOG_DEBUG(logger_, "Writing" << KVLOG(write_msg_->msg.size()));

  // We don't want to include tcp transmission time.
  histograms_.send_time_in_queue->recordAtomic(durationInMicros(write_msg_->send_time));

  auto self = shared_from_this();
  auto start = std::chrono::steady_clock::now();
  asio::async_write(
      *socket_,
      asio::buffer(write_msg_->msg),
      asio::bind_executor(strand_, [this, self, start](const asio::error_code& ec, auto /*bytes_written*/) {
        if (disposed_) return;
        if (ec) {
          if (ec == asio::error::operation_aborted) {
            // The socket has already been cleaned up and any references are invalid. Just return.
            LOG_DEBUG(logger_, "Operation aborted: " << KVLOG(peer_id_.value(), disposed_));
            return;
          }
          LOG_WARN(logger_,
                   "Write failed to node " << peer_id_.value() << " for message with size " << write_msg_->msg.size()
                                           << ": " << ec.message());
          return dispose();
        }

        // The write succeeded.
        histograms_.async_write->recordAtomic(durationInMicros(start));
        write_timer_.cancel();
        histograms_.sent_msg_size->recordAtomic(static_cast<int64_t>(write_msg_->msg.size()));
        write_msg_ = nullptr;
        write_msg_used_ = false;
        write(write_queue_.pop());
      }));
  startWriteTimer();
}

void AsyncTlsConnection::createSSLSocket(asio::ip::tcp::socket&& socket) {
  socket_ = std::make_unique<SSL_SOCKET>(io_context_, ssl_context_);
  socket_->lowest_layer() = std::move(socket);
}

void AsyncTlsConnection::initClientSSLContext(NodeNum destination) {
  auto self = std::weak_ptr(shared_from_this());
  ssl_context_.set_verify_mode(asio::ssl::verify_peer);

  namespace fs = boost::filesystem;
  fs::path path;
  try {
    path = fs::path(config_.certificatesRootPath) / fs::path(std::to_string(config_.selfId)) / "client";
  } catch (std::exception& e) {
    LOG_FATAL(logger_, "Failed to construct filesystem path: " << e.what());
    ConcordAssert(false);
  }

  asio::error_code ec;
  ssl_context_.set_verify_callback(
      [this, self, destination](auto /*preverified*/, auto& ctx) -> bool {
        if (self.expired()) return false;
        return verifyCertificateClient(ctx, destination);
      },
      ec);
  if (ec) {
    LOG_FATAL(logger_, "Unable to set client verify callback" << ec.message());
    ConcordAssert(false);
  }

  try {
    ssl_context_.use_certificate_chain_file((path / "client.cert").string());
    const std::string pk = decryptPrivateKey(path);
    ssl_context_.use_private_key(asio::const_buffer(pk.c_str(), pk.size()), asio::ssl::context::pem);
  } catch (const boost::system::system_error& e) {
    LOG_FATAL(logger_, "Failed to load certificate or private key files from path: " << path << " : " << e.what());
    ConcordAssert(false);
  }

  // Only allow using the strongest cipher suites.
  if (!SSL_CTX_set_ciphersuites(ssl_context_.native_handle(), config_.cipherSuite.c_str())) {
    LOG_ERROR(logger_, "Failed to set TLS cipher suite from config: " << config_.cipherSuite.c_str());

    // Setting to Default
    if (!SSL_CTX_set_ciphersuites(ssl_context_.native_handle(), "TLS_AES_256_GCM_SHA384"))
      LOG_FATAL(logger_, "Failed to set default TLS cipher suite");
  }
}

void AsyncTlsConnection::initServerSSLContext() {
  auto self = std::weak_ptr(shared_from_this());
  ssl_context_.set_verify_mode(asio::ssl::verify_peer | asio::ssl::verify_fail_if_no_peer_cert);
  ssl_context_.set_options(asio::ssl::context::default_workarounds | asio::ssl::context::no_sslv2 |
                           asio::ssl::context::no_sslv3 | asio::ssl::context::no_tlsv1 |
                           asio::ssl::context::no_tlsv1_1 | asio::ssl::context::no_tlsv1_2 |
                           asio::ssl::context::single_dh_use);

  asio::error_code ec;
  ssl_context_.set_verify_callback(
      [this, self](auto /*preverified*/, auto& ctx) -> bool {
        if (self.expired()) return false;
        return verifyCertificateServer(ctx);
      },
      ec);
  if (ec) {
    LOG_FATAL(logger_, "Unable to set server verify callback" << ec.message());
    ConcordAssert(false);
  }

  namespace fs = boost::filesystem;
  fs::path path;
  try {
    path = fs::path(config_.certificatesRootPath) / fs::path(std::to_string(config_.selfId)) / fs::path("server");
  } catch (std::exception& e) {
    LOG_FATAL(logger_, "Failed to construct filesystem path: " << e.what());
    ConcordAssert(false);
  }

  try {
    ssl_context_.use_certificate_chain_file((path / fs::path("server.cert")).string());
    const std::string pk = decryptPrivateKey(path);
    ssl_context_.use_private_key(asio::const_buffer(pk.c_str(), pk.size()), asio::ssl::context::pem);
  } catch (const boost::system::system_error& e) {
    LOG_FATAL(logger_, "Failed to load certificate or private key files from path: " << path << " : " << e.what());
    ConcordAssert(false);
  }

  EC_KEY* ecdh = EC_KEY_new_by_curve_name(NID_secp384r1);
  if (!ecdh) {
    LOG_FATAL(logger_, "Unable to create EC");
    ConcordAssert(false);
  }

  if (1 != SSL_CTX_set_tmp_ecdh(ssl_context_.native_handle(), ecdh)) {
    LOG_FATAL(logger_, "Unable to set temp EC params");
    ConcordAssert(false);
  }

  // As OpenSSL does reference counting, it should be safe to free the key.
  // However, there is no explicit info on this point in the openssl docs.
  // This info is from various online sources and examples
  EC_KEY_free(ecdh);

  // Only allow using the strongest cipher suites.
  if (!SSL_CTX_set_ciphersuites(ssl_context_.native_handle(), config_.cipherSuite.c_str())) {
    LOG_ERROR(logger_, "Failed to set TLS cipher suite from config : " << config_.cipherSuite.c_str());

    // Setting to default
    if (!SSL_CTX_set_ciphersuites(ssl_context_.native_handle(), "TLS_AES_256_GCM_SHA384"))
      LOG_FATAL(logger_, "Failed to set default TLS cipher suite");
  }
}

bool AsyncTlsConnection::verifyCertificateClient(asio::ssl::verify_context& ctx, NodeNum expected_dest_id) {
  if (X509_V_ERR_DEPTH_ZERO_SELF_SIGNED_CERT != X509_STORE_CTX_get_error(ctx.native_handle())) {
    return false;
  }
  std::string subject(256, 0);
  X509* cert = X509_STORE_CTX_get_current_cert(ctx.native_handle());
  if (!cert) {
    LOG_WARN(logger_, "No certificate from server at node " << expected_dest_id);
    return false;
  }
  X509_NAME_oneline(X509_get_subject_name(cert), subject.data(), 256);
  auto [valid, _] = checkCertificate(cert, "server", subject, expected_dest_id);
  (void)_;  // unused variable hack
  return valid;
}

bool AsyncTlsConnection::verifyCertificateServer(asio::ssl::verify_context& ctx) {
  if (X509_V_ERR_DEPTH_ZERO_SELF_SIGNED_CERT != X509_STORE_CTX_get_error(ctx.native_handle())) {
    return false;
  }
  static constexpr size_t SIZE = 512;
  std::string subject(SIZE, 0);
  X509* cert = X509_STORE_CTX_get_current_cert(ctx.native_handle());
  if (!cert) {
    LOG_WARN(logger_, "No certificate from client");
    return false;
  }
  X509_NAME_oneline(X509_get_subject_name(cert), subject.data(), SIZE);
  auto [valid, peer_id] = checkCertificate(cert, "client", std::string(subject), std::nullopt);
  peer_id_ = peer_id;
  return valid;
}

std::pair<bool, NodeNum> AsyncTlsConnection::checkCertificate(X509* receivedCert,
                                                              std::string connectionType,
                                                              const std::string& subject,
                                                              std::optional<NodeNum> expectedPeerId) {
  // First, perform a basic sanity test, in order to eliminate a disk read if the certificate is
  // unknown.
  //
  // The certificate must have a node id, as we put it in `OU` field on creation.
  //
  // Since we use pinning we must know who the remote peer is.
  // `peerIdPrefixLength` stands for the length of 'OU=' substring
  int peerIdPrefixLength = 3;
  std::regex r("OU=\\d*", std::regex_constants::icase);
  std::smatch sm;
  regex_search(subject, sm, r);
  if (sm.length() <= peerIdPrefixLength) {
    LOG_WARN(logger_, "OU not found or empty: " << subject);
    return std::make_pair(false, 0);
  }

  auto remPeer = sm.str().substr(peerIdPrefixLength, sm.str().length() - peerIdPrefixLength);
  if (0 == remPeer.length()) {
    LOG_WARN(logger_, "OU empty " << subject);
    return std::make_pair(false, 0);
  }

  NodeNum remotePeerId;
  try {
    remotePeerId = stoul(remPeer, nullptr);
  } catch (const std::invalid_argument& ia) {
    LOG_WARN(logger_, "cannot convert OU, " << subject << ", " << ia.what());
    return std::make_pair(false, 0);
  } catch (const std::out_of_range& e) {
    LOG_WARN(logger_, "cannot convert OU, " << subject << ", " << e.what());
    return std::make_pair(false, 0);
  }

  // If the server has been verified, check that the peers match.
  if (expectedPeerId) {
    if (remotePeerId != expectedPeerId) {
      LOG_WARN(logger_, "Peers don't match, expected: " << expectedPeerId.value() << ", received: " << remPeer);
      return std::make_pair(false, remotePeerId);
    }
  }

  // the actual pinning - read the correct certificate from the disk and
  // compare it to the received one
  namespace fs = boost::filesystem;
  fs::path path;
  try {
    path = fs::path(config_.certificatesRootPath) / std::to_string(remotePeerId) / connectionType /
           std::string(connectionType + ".cert");
  } catch (std::exception& e) {
    LOG_FATAL(logger_, "Failed to construct filesystem path: " << e.what());
    ConcordAssert(false);
  }

  auto deleter = [](FILE* fp) {
    if (fp) fclose(fp);
  };
  std::unique_ptr<FILE, decltype(deleter)> fp(fopen(path.c_str(), "r"), deleter);
  if (!fp) {
    LOG_ERROR(logger_, "Certificate file not found, path: " << path);
    return std::make_pair(false, remotePeerId);
  }

  X509* localCert = PEM_read_X509(fp.get(), NULL, NULL, NULL);
  if (!localCert) {
    LOG_ERROR(logger_, "Cannot parse certificate, path: " << path);
    return std::make_pair(false, remotePeerId);
  }

  // this is actual comparison, compares hash of 2 certs
  int res = X509_cmp(receivedCert, localCert);
  X509_free(localCert);
  if (res == 0) {
    // We don't put a log message here, because it will be called for each cert in the chain, resulting in duplicates.
    // Instead we log in onXXXHandshakeComplete callbacks it TlsTcpImpl.
    return std::make_pair(true, remotePeerId);
  }
  LOG_WARN(logger_,
           "X509_cmp failed at node: " << config_.selfId << ", type: " << connectionType << ", peer: " << remotePeerId
                                       << " res=" << res);
  return std::make_pair(false, remotePeerId);
}

using namespace concord::secretsmanager;

const std::string AsyncTlsConnection::decryptPrivateKey(const boost::filesystem::path& path) {
  namespace fs = boost::filesystem;
  std::string pkpath;

  std::unique_ptr<ISecretsManagerImpl> secrets_manager;
  if (config_.secretData) {
    pkpath = (path / fs::path("pk.pem.enc")).string();
    secrets_manager.reset(new SecretsManagerEnc(config_.secretData.value()));
  } else {
    pkpath = (path / fs::path("pk.pem")).string();
    secrets_manager.reset(new SecretsManagerPlain());
  }

  auto decBuf = secrets_manager->decryptFile(pkpath);
  if (!decBuf) {
    throw std::runtime_error("Error decrypting " + pkpath);
  }

  return *decBuf;
}
void AsyncTlsConnection::close() {
  std::lock_guard<std::mutex> lock(shutdown_lock_);
  if (closed_) return;
  socket_->lowest_layer().close();
  closed_ = true;
}

}  // namespace bft::communication::tls
