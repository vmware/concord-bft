// Concord
//
// Copyright (c) 2020-2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the
// LICENSE file.

#pragma once

#include <asio/io_context.hpp>
#include <vector>
#include <optional>

#include <asio.hpp>
#include <asio/ssl.hpp>
#include "util/filesystem.hpp"

#include "communication/CommDefs.hpp"
#include "Logger.hpp"
#include "TlsConnectionManager.h"
#include "TlsDiagnostics.h"
#include "TlsWriteQueue.h"
#include "ReplicaConfig.hpp"

namespace bft::communication::tls {

typedef asio::ssl::stream<asio::ip::tcp::socket> SSL_SOCKET;

class AsyncTlsConnection : public std::enable_shared_from_this<AsyncTlsConnection> {
 public:
  static constexpr std::chrono::seconds READ_TIMEOUT = std::chrono::seconds(10);
  static constexpr std::chrono::seconds WRITE_TIMEOUT = READ_TIMEOUT;

  // We require a factory function because we can't call shared_from_this() in the constructor.
  //
  // In order to call shared_from_this(), there must already be a shared pointer wrapping `this`.
  // In this case, shared_from_this() is required for registering verification callbacks in the SSL context
  // initialization functions.
  static std::shared_ptr<AsyncTlsConnection> create(asio::io_context& io_context,
                                                    asio::ip::tcp::socket&& socket,
                                                    IReceiver* receiver,
                                                    ConnectionManager& conn_mgr,
                                                    TlsTcpConfig& config,
                                                    TlsStatus& status,
                                                    Recorders& histograms) {
    auto conn = std::make_shared<AsyncTlsConnection>(io_context, receiver, conn_mgr, config, status, histograms);
    conn->initServerSSLContext();
    conn->createSSLSocket(std::move(socket));
    return conn;
  }

  static std::shared_ptr<AsyncTlsConnection> create(asio::io_context& io_context,
                                                    asio::ip::tcp::socket&& socket,
                                                    IReceiver* receiver,
                                                    ConnectionManager& conn_mgr,
                                                    NodeNum destination,
                                                    TlsTcpConfig& config,
                                                    TlsStatus& status,
                                                    Recorders& histograms) {
    auto conn =
        std::make_shared<AsyncTlsConnection>(io_context, receiver, conn_mgr, destination, config, status, histograms);
    conn->initClientSSLContext(destination);
    conn->createSSLSocket(std::move(socket));
    return conn;
  }

  // Constructor for an accepting (server) connection.
  AsyncTlsConnection(asio::io_context& io_context,
                     IReceiver* receiver,
                     ConnectionManager& conn_mgr,
                     TlsTcpConfig& config,
                     TlsStatus& status,
                     Recorders& histograms)
      : logger_(logging::getLogger("concord-bft.tls.conn")),
        io_context_(io_context),
        strand_(asio::make_strand(io_context_)),
        ssl_context_(asio::ssl::context::tlsv13_server),
        receiver_(receiver),
        connection_manager_(conn_mgr),
        read_timer_(io_context_),
        write_timer_(io_context_),
        read_msg_(config.bufferLength_),
        config_(config),
        status_(status),
        histograms_(histograms),
        write_queue_(histograms_) {}

  // Constructor for a connecting (client) connection.
  AsyncTlsConnection(asio::io_context& io_context,
                     IReceiver* receiver,
                     ConnectionManager& conn_mgr,
                     NodeNum peer_id,
                     TlsTcpConfig& config,
                     TlsStatus& status,
                     Recorders& histograms)
      : logger_(logging::getLogger("concord-bft.tls.conn")),
        io_context_(io_context),
        strand_(asio::make_strand(io_context_)),
        ssl_context_(asio::ssl::context::tlsv13_client),
        peer_id_(peer_id),
        receiver_(receiver),
        connection_manager_(conn_mgr),
        read_timer_(io_context_),
        write_timer_(io_context_),
        read_msg_(config.bufferLength_),
        config_(config),
        status_(status),
        histograms_(histograms),
        write_queue_(histograms_) {
    write_queue_.setDestination(peer_id);
  }

  void setPeerId(NodeNum peer_id) {
    peer_id_ = peer_id;
    write_queue_.setDestination(peer_id);
  }

  std::optional<NodeNum> getPeerId() const { return peer_id_; }

  SSL_SOCKET& getSocket() { return *socket_.get(); }

  // Wrapper function to be called from the ConnMgr.
  void send(std::shared_ptr<OutgoingMsg>&& msg);

  // Wrapper function to be called from the ConnMgr.
  void startReading();

  // Every messsage is preceded by a 4 byte message header that we must read.
  // `bytes_already_read` == `std::nullopt` if this is the first read call.
  void readMsgSizeHeader();
  void readMsgSizeHeader(std::optional<size_t> bytes_already_read);

  // Write this message in strand_ , or enqueue it if there is already a message being written.
  void write(std::shared_ptr<OutgoingMsg>);

  // Wrapper function to be called from the ConnMgr strand.
  void remoteDispose();
  // Clean up the connection
  void dispose(bool close_connection = true);

  void close();

 private:
  // We know the size of the message and that a message should be forthcoming. We start a timer and
  // ensure we read all remaining bytes within a given timeout. If we read the full message we
  // inform the `receiver_`, otherwise we `dispose` of the connection.
  void readMsg();

  // Return the recently read size header as an integer. Assume network byte order.
  uint32_t getReadMsgSize();

  // Return the recently read endpoint number from header as an integer. Assume network byte order
  NodeNum getReadMsgEndpointNum() const;

  void startReadTimer();
  void startWriteTimer();

  void createSSLSocket(asio::ip::tcp::socket&&);
  void initClientSSLContext(NodeNum destination);
  void initServerSSLContext();

  // Callbacks triggered from asio for certificate validation
  // On the server side we don't know the identity of the accepted connection until we verify the certificate and read
  // the node id.
  bool verifyCertificateClient(asio::ssl::verify_context& ctx, NodeNum expected_dest_id);
  bool verifyCertificateServer(asio::ssl::verify_context& ctx);

  // Certificate pinning
  //
  // Check for a specific certificate and do not rely on chain authentication.
  //
  // Return true along with the actual node id if verification succeeds, (false, 0) if not.
  std::pair<bool, NodeNum> checkCertificate(X509* received_cert, std::optional<NodeNum> expected_peer_id);

  const std::string decryptPrivateKey(const fs::path& path);

  logging::Logger logger_;
  asio::io_context& io_context_;

  // All reads and writes are executed in this strand.
  // This strand is only used after a connection is authenticated, which occurs in the `ConnMgr` strand.
  asio::strand<asio::io_context::executor_type> strand_;

  // We can't rely on timer callbacks to actually be cancelled properly and return
  // `boost::asio::error::operation_aborted`. There is a race condition where a callback may already
  // be queued before the timer was cancelled. If we destroy the socket due to a failed write, and
  // then the callback fires, we may have already closed the socket, and we don't want to do that
  // twice.
  //
  // We also guard against a race condition between this strand closing the socket and the ConnMgr
  // strand posting a message to close the connection.
  bool disposed_ = false;

  asio::ssl::context ssl_context_;

  std::unique_ptr<SSL_SOCKET> socket_;
  std::optional<NodeNum> peer_id_ = std::nullopt;

  IReceiver* receiver_ = nullptr;
  ConnectionManager& connection_manager_;

  asio::steady_timer read_timer_;
  asio::steady_timer write_timer_;

  // On every read, we must read the size of the incoming message first. This buffer stores that size.
  std::array<char, MSG_HEADER_SIZE> read_size_buf_;

  // Last read message
  std::vector<char> read_msg_;

  // Message being currently written.
  std::atomic_bool write_msg_used_{false};
  std::shared_ptr<OutgoingMsg> write_msg_;

  TlsTcpConfig& config_;
  TlsStatus& status_;
  Recorders& histograms_;
  WriteQueue write_queue_;
  std::mutex shutdown_lock_;
  bool closed_ = false;
};

}  // namespace bft::communication::tls
