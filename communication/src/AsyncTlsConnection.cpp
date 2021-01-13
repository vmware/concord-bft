// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the
// LICENSE file.

#include <regex>

#include <arpa/inet.h>
#include <boost/filesystem.hpp>
#include <chrono>
#include <optional>

#include "AsyncTlsConnection.h"
#include "TlsTcpImpl.h"
#include "TlsDiagnostics.h"

using concord::diagnostics::TimeRecorder;

namespace bft::communication {

void AsyncTlsConnection::readMsgSizeHeader(std::optional<size_t> bytes_already_read) {
  LOG_DEBUG(logger_, KVLOG(peer_id_.value()));
  auto self = shared_from_this();
  const size_t offset = bytes_already_read ? bytes_already_read.value() : 0;
  const size_t bytes_remaining = MSG_HEADER_SIZE - offset;
  auto buf = boost::asio::buffer(read_size_buf_.data() + offset, bytes_remaining);
  const auto start_read = std::chrono::steady_clock::now();

  socket_->async_read_some(
      buf,
      [this, self, bytes_already_read, bytes_remaining, start_read](const auto& error_code, auto bytes_transferred) {
        auto interval =
            std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start_read);
        tlsTcpImpl_.histograms_.time_between_reads->record(interval.count());
        if (disposed_) {
          return;
        }
        if (error_code) {
          if (error_code == boost::asio::error::operation_aborted) {
            // The socket has already been cleaned up and any references are invalid. Just return.
            LOG_DEBUG(logger_, "Operation aborted: " << KVLOG(peer_id_.value(), disposed_));
            return;
          }
          // Remove the connection as it is no longer valid, and then close it, cancelling any ongoing operations.
          LOG_ERROR(logger_,
                    "Reading message size header failed for node " << peer_id_.value() << ": " << error_code.message());
          return dispose();
        }

        if (!bytes_already_read) {
          startReadTimer();
        }

        if (bytes_transferred != bytes_remaining) {
          LOG_DEBUG(
              logger_,
              "Short read on messsage header occurred" << KVLOG(peer_id_.value(), bytes_remaining, bytes_transferred));
          readMsgSizeHeader(MSG_HEADER_SIZE - (bytes_remaining - bytes_transferred));
        } else {
          // The message size header was read completely.
          if (getReadMsgSize() > tlsTcpImpl_.config_.bufferLength) {
            LOG_ERROR(logger_,
                      "Message Size: " << getReadMsgSize() << " exceeds maximum: " << tlsTcpImpl_.config_.bufferLength
                                       << " for node " << peer_id_.value());
            return dispose();
          }

          readMsg();
        }
      });
}

void AsyncTlsConnection::readMsgSizeHeader() { readMsgSizeHeader(std::nullopt); }

void AsyncTlsConnection::readMsg() {
  auto msg_size = getReadMsgSize();
  LOG_DEBUG(logger_, KVLOG(peer_id_.value(), msg_size, (void*)read_msg_.data()));
  auto self = shared_from_this();
  async_read(*socket_,
             boost::asio::buffer(read_msg_.data(), msg_size),
             [this, self, msg_size](const boost::system::error_code& error_code, auto bytes_transferred) {
               if (disposed_) {
                 return;
               }
               if (error_code) {
                 if (error_code == boost::asio::error::operation_aborted) {
                   LOG_DEBUG(logger_, "Operation aborted: " << KVLOG(peer_id_.value(), disposed_));
                   // The socket has already been cleaned up and any references are invalid. Just return.
                   return;
                 }
                 // Remove the connection as it is no longer valid, and then close it, cancelling any ongoing
                 // operations.
                 LOG_ERROR(logger_,
                           "Reading message of size <<" << getReadMsgSize() << " failed for node " << peer_id_.value()
                                                        << ": " << error_code.message());
                 return dispose();
               }

               // This is a bug in boost::asio (boost 1.65) that was fixed in later versions. `async_read` is
               // only supposed to return a full buffer or an error. However, this bug allows, no error but a
               // return of exactly zero bytes read. In this case, we don't know the state of the
               // stream, so it's safest to dispose of the connection.
               // https://github.com/boostorg/asio/pull/182
               // https://github.com/boostorg/asio/commit/ce7e3bbf4b7070b8292df50d3514c34ce0353684
               // https://github.com/chriskohlhoff/asio/commit/57b2ef19b013dd1fd8660af28398d3d332d1ea97
               if (bytes_transferred == 0) {
                 LOG_ERROR(logger_, "Short read (0 bytes) of message occurred" << KVLOG(peer_id_.value(), msg_size));
                 return dispose();
               }

               // This would be a much more severe bug that is undocumented, and also that we have
               // never seen. This check is here just in case, because we do not want to revert our
               // composed operation (async_read) to partial read (async_read_some) methods and make
               // things more complex.
               if (bytes_transferred != msg_size) {
                 LOG_ERROR(logger_,
                           "ASIO violated the contract for async_read and did not fill the buffer. Closing connection."
                               << KVLOG(peer_id_.value(), msg_size, bytes_transferred));
                 return dispose();
               }

               // The Read succeeded.
               boost::system::error_code _ec;
               LOG_DEBUG(logger_, "Cancelling read timer: " << KVLOG(peer_id_.value(), (void*)read_msg_.data()));
               read_timer_.cancel(_ec);
               tlsTcpImpl_.histograms_.received_msg_size->record(bytes_transferred);
               {
                 TimeRecorder scoped_timer(*tlsTcpImpl_.histograms_.read_enqueue_time);
                 receiver_->onNewMessage(peer_id_.value(), read_msg_.data(), bytes_transferred);
               }
               if (tlsTcpImpl_.config_.statusCallback && tlsTcpImpl_.isReplica(peer_id_.value())) {
                 PeerConnectivityStatus pcs{};
                 pcs.peerId = peer_id_.value();
                 pcs.statusType = StatusType::MessageReceived;
                 tlsTcpImpl_.config_.statusCallback(pcs);
               }
               readMsgSizeHeader();
             });
}

void AsyncTlsConnection::startReadTimer() {
  LOG_DEBUG(logger_, KVLOG(peer_id_.value()));
  auto self = shared_from_this();
  read_timer_.expires_from_now(READ_TIMEOUT);
  read_timer_.async_wait([this, self](const boost::system::error_code& ec) {
    if (ec == boost::asio::error::operation_aborted || disposed_) {
      // The socket has already been cleaned up and any references are invalid. Just return.
      LOG_DEBUG(logger_, "Operation aborted: " << KVLOG(peer_id_.value(), disposed_));
      return;
    }
    LOG_WARN(logger_, "Read timeout from node " << peer_id_.value() << ": " << ec.message());
    dispose();
  });
}

void AsyncTlsConnection::startWriteTimer() {
  auto self = shared_from_this();
  write_timer_.expires_from_now(WRITE_TIMEOUT);
  write_timer_.async_wait([this, self](const boost::system::error_code& ec) {
    if (ec == boost::asio::error::operation_aborted || disposed_) {
      // The socket has already been cleaned up and any references are invalid. Just return.
      LOG_DEBUG(logger_, "Operation aborted: " << KVLOG(peer_id_.value(), disposed_));
      return;
    }
    LOG_WARN(logger_, "Write timeout to node " << peer_id_.value() << ": " << ec.message());
    dispose();
  });
}

uint32_t AsyncTlsConnection::getReadMsgSize() {
  LOG_DEBUG(logger_, KVLOG(peer_id_.value(), (void*)read_size_buf_.data()));
  // We send in network byte order
  // We must use memcpy to get aligned access
  uint32_t num;
  memcpy(&num, read_size_buf_.data(), 4);
  return ntohl(num);
}

void AsyncTlsConnection::dispose() {
  ConcordAssert(!disposed_);
  LOG_WARN(logger_, "Closing connection to node " << peer_id_.value());
  disposed_ = true;
  boost::system::error_code _;
  read_timer_.cancel(_);
  write_timer_.cancel(_);
  tlsTcpImpl_.closeConnection(peer_id_.value());
}

void AsyncTlsConnection::send(std::vector<char>&& raw_msg) {
  LOG_DEBUG(logger_, KVLOG(peer_id_.value()));
  std::lock_guard<std::mutex> guard(write_lock_);
  auto size = raw_msg.size();
  if (queued_size_in_bytes_ + size > MAX_QUEUE_SIZE_IN_BYTES) {
    LOG_WARN(logger_, "Outgoing Queue is full. Dropping message with size: " << raw_msg.size());
    return;
  }
  out_queue_.push_back(OutgoingMsg(std::move(raw_msg)));
  queued_size_in_bytes_ += size;

  tlsTcpImpl_.histograms_.write_queue_len->record(out_queue_.size());
  tlsTcpImpl_.histograms_.write_queue_size_in_bytes->record(queued_size_in_bytes_);

  // If out_queue_.size() > 1 then the io_thread is already writing. We don't want to initiate two
  // simultaneous async_write calls to the same socket. We also must ensure that this write call
  // runs in a strand in io_service_ because the async_write it contains cannot safely run in
  // another thread. WE use io_service_.post() for this.
  if (out_queue_.size() == 1) {
    auto self = shared_from_this();
    io_service_.post([this, self]() { write(); });
  }

  if (tlsTcpImpl_.config_.statusCallback && tlsTcpImpl_.isReplica()) {
    PeerConnectivityStatus pcs{};
    pcs.peerId = tlsTcpImpl_.config_.selfId;
    pcs.statusType = StatusType::MessageSent;
    tlsTcpImpl_.config_.statusCallback(pcs);
  }
}

// Invariant: write_lock_ is held
void AsyncTlsConnection::dropStaleMsgs() {
  ConcordAssert(!out_queue_.empty());
  while (out_queue_.front().send_time + STALE_MESSAGE_TIMEOUT < std::chrono::steady_clock::now()) {
    auto diff = std::chrono::steady_clock::now() - out_queue_.front().send_time;
    LOG_WARN(logger_,
             "Message queued for peer " << peer_id_.value() << " for "
                                        << std::chrono::duration_cast<std::chrono::seconds>(diff).count()
                                        << " seconds, with size: " << out_queue_.front().msg.size()
                                        << " dropped. Message is stale: Exceeded threshold of "
                                        << STALE_MESSAGE_TIMEOUT.count() << " seconds.");
    queued_size_in_bytes_ -= out_queue_.front().msg.size();
    tlsTcpImpl_.histograms_.send_time_in_queue->record(
        std::chrono::duration_cast<std::chrono::nanoseconds>(diff).count());
    out_queue_.pop_front();
    if (out_queue_.empty()) {
      return;
    }
  }
}

void AsyncTlsConnection::write() {
  std::lock_guard<std::mutex> guard(write_lock_);
  dropStaleMsgs();
  if (out_queue_.empty()) return;

  // We don't want to include tcp transmission time.
  auto diff = std::chrono::steady_clock::now() - out_queue_.front().send_time;
  tlsTcpImpl_.histograms_.send_time_in_queue->record(
      std::chrono::duration_cast<std::chrono::nanoseconds>(diff).count());

  auto self = shared_from_this();
  startWriteTimer();
  LOG_DEBUG(logger_, "Before async write: " << KVLOG(peer_id_.value(), out_queue_.front().msg.size()));
  boost::asio::async_write(
      *socket_,
      boost::asio::buffer(out_queue_.front().msg),
      [this, self](const boost::system::error_code& ec, auto bytes_written) {
        if (disposed_) {
          return;
        }
        if (ec) {
          if (ec == boost::asio::error::operation_aborted) {
            // The socket has already been cleaned up and any references are invalid. Just return.
            LOG_DEBUG(logger_, "Operation aborted: " << KVLOG(peer_id_.value(), disposed_));
            return;
          }
          LOG_WARN(logger_,
                   "Write failed to node " << peer_id_.value() << " for message with size "
                                           << out_queue_.front().msg.size() << ": " << ec.message());
          return dispose();
        }
        // The write succeeded.
        boost::system::error_code _ec;
        write_timer_.cancel(_ec);

        LOG_DEBUG(logger_,
                  "Successful async write: " << KVLOG(peer_id_.value(), out_queue_.front().msg.size(), bytes_written));
        auto size = out_queue_.front().msg.size();
        // Minimize time holding lock. We also can't hold the lock when we call write.
        bool continue_writing = false;
        {
          std::lock_guard<std::mutex> guard(write_lock_);
          out_queue_.pop_front();
          continue_writing = !out_queue_.empty();
          queued_size_in_bytes_ -= size;
        }
        tlsTcpImpl_.histograms_.sent_msg_size->record(size);
        if (continue_writing) {
          write();
        }
      });
}

void AsyncTlsConnection::createSSLSocket(boost::asio::ip::tcp::socket&& socket) {
  socket_ = std::make_unique<SSL_SOCKET>(io_service_, ssl_context_);
  socket_->lowest_layer() = std::move(socket);
}

void AsyncTlsConnection::initClientSSLContext(NodeNum destination) {
  auto self = std::weak_ptr(shared_from_this());
  ssl_context_.set_verify_mode(boost::asio::ssl::verify_peer);

  namespace fs = boost::filesystem;
  fs::path path;
  try {
    path = fs::path(tlsTcpImpl_.config_.certificatesRootPath) / fs::path(std::to_string(tlsTcpImpl_.config_.selfId)) /
           "client";
  } catch (std::exception& e) {
    LOG_FATAL(logger_, "Failed to construct filesystem path: " << e.what());
    ConcordAssert(false);
  }

  boost::system::error_code ec;
  ssl_context_.set_verify_callback(
      [this, self, destination](auto preverified, auto& ctx) -> bool {
        if (self.expired()) return false;
        return verifyCertificateClient(preverified, ctx, destination);
      },
      ec);
  if (ec) {
    LOG_FATAL(logger_, "Unable to set client verify callback" << ec.message());
    ConcordAssert(false);
  }

  try {
    ssl_context_.use_certificate_chain_file((path / "client.cert").string());
    ssl_context_.use_private_key_file((path / "pk.pem").string(), boost::asio::ssl::context::pem);
  } catch (const boost::system::system_error& e) {
    LOG_FATAL(logger_, "Failed to load certificate or private key files from path: " << path << " : " << e.what());
    ConcordAssert(false);
  }

  // Only allow using the strongest cipher suites.
  SSL_CTX_set_cipher_list(ssl_context_.native_handle(), tlsTcpImpl_.config_.cipherSuite.c_str());
}

void AsyncTlsConnection::initServerSSLContext() {
  auto self = std::weak_ptr(shared_from_this());
  ssl_context_.set_verify_mode(boost::asio::ssl::verify_peer | boost::asio::ssl::verify_fail_if_no_peer_cert);
  ssl_context_.set_options(boost::asio::ssl::context::default_workarounds | boost::asio::ssl::context::no_sslv2 |
                           boost::asio::ssl::context::no_sslv3 | boost::asio::ssl::context::no_tlsv1 |
                           boost::asio::ssl::context::no_tlsv1_1 | boost::asio::ssl::context::single_dh_use);

  boost::system::error_code ec;
  ssl_context_.set_verify_callback(
      [this, self](auto preverified, auto& ctx) -> bool {
        if (self.expired()) return false;
        return verifyCertificateServer(preverified, ctx);
      },
      ec);
  if (ec) {
    LOG_FATAL(logger_, "Unable to set server verify callback" << ec.message());
    ConcordAssert(false);
  }

  namespace fs = boost::filesystem;
  fs::path path;
  try {
    path = fs::path(tlsTcpImpl_.config_.certificatesRootPath) / fs::path(std::to_string(tlsTcpImpl_.config_.selfId)) /
           fs::path("server");
  } catch (std::exception& e) {
    LOG_FATAL(logger_, "Failed to construct filesystem path: " << e.what());
    ConcordAssert(false);
  }

  try {
    ssl_context_.use_certificate_chain_file((path / fs::path("server.cert")).string());
    ssl_context_.use_private_key_file((path / fs::path("pk.pem")).string(), boost::asio::ssl::context::pem);
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
  SSL_CTX_set_cipher_list(ssl_context_.native_handle(), tlsTcpImpl_.config_.cipherSuite.c_str());
}

bool AsyncTlsConnection::verifyCertificateClient(bool preverified,
                                                 boost::asio::ssl::verify_context& ctx,
                                                 NodeNum expected_dest_id) {
  if (X509_V_ERR_DEPTH_ZERO_SELF_SIGNED_CERT != X509_STORE_CTX_get_error(ctx.native_handle())) {
    return false;
  }
  std::string subject(256, 0);
  X509* cert = X509_STORE_CTX_get_current_cert(ctx.native_handle());
  if (!cert) {
    LOG_ERROR(logger_, "No certificate from server at node " << expected_dest_id);
    return false;
  }
  X509_NAME_oneline(X509_get_subject_name(cert), subject.data(), 256);
  auto [valid, _] = checkCertificate(cert, "server", subject, expected_dest_id);
  (void)_;  // unused variable hack
  return valid;
}

bool AsyncTlsConnection::verifyCertificateServer(bool preverified, boost::asio::ssl::verify_context& ctx) {
  if (X509_V_ERR_DEPTH_ZERO_SELF_SIGNED_CERT != X509_STORE_CTX_get_error(ctx.native_handle())) {
    return false;
  }
  static constexpr size_t SIZE = 512;
  std::string subject(SIZE, 0);
  X509* cert = X509_STORE_CTX_get_current_cert(ctx.native_handle());
  if (!cert) {
    LOG_ERROR(logger_, "No certificate from client");
    return false;
  }
  X509_NAME_oneline(X509_get_subject_name(cert), subject.data(), SIZE);
  auto [valid, peer_id] = checkCertificate(cert, "client", std::string(subject), std::nullopt);
  peer_id_ = peer_id;
  return valid;
}

std::pair<bool, NodeNum> AsyncTlsConnection::checkCertificate(X509* receivedCert,
                                                              std::string connectionType,
                                                              std::string subject,
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
    LOG_ERROR(logger_, "OU not found or empty: " << subject);
    return std::make_pair(false, 0);
  }

  auto remPeer = sm.str().substr(peerIdPrefixLength, sm.str().length() - peerIdPrefixLength);
  if (0 == remPeer.length()) {
    LOG_ERROR(logger_, "OU empty " << subject);
    return std::make_pair(false, 0);
  }

  NodeNum remotePeerId;
  try {
    remotePeerId = stoul(remPeer, nullptr);
  } catch (const std::invalid_argument& ia) {
    LOG_ERROR(logger_, "cannot convert OU, " << subject << ", " << ia.what());
    return std::make_pair(false, 0);
  } catch (const std::out_of_range& e) {
    LOG_ERROR(logger_, "cannot convert OU, " << subject << ", " << e.what());
    return std::make_pair(false, 0);
  }

  // If the server has been verified, check that the peers match.
  if (expectedPeerId) {
    if (remotePeerId != expectedPeerId) {
      LOG_ERROR(logger_, "Peers don't match, expected: " << expectedPeerId.value() << ", received: " << remPeer);
      return std::make_pair(false, remotePeerId);
    }
  }

  // the actual pinning - read the correct certificate from the disk and
  // compare it to the received one
  namespace fs = boost::filesystem;
  fs::path path;
  try {
    path = fs::path(tlsTcpImpl_.config_.certificatesRootPath) / std::to_string(remotePeerId) / connectionType /
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
  LOG_ERROR(logger_,
            "X509_cmp failed at node: " << tlsTcpImpl_.config_.selfId << ", type: " << connectionType
                                        << ", peer: " << remotePeerId << " res=" << res);
  return std::make_pair(false, remotePeerId);
}

}  // namespace bft::communication
