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

#pragma once

#include <deque>
#include <vector>
#include <mutex>
#include <optional>

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/asio/steady_timer.hpp>

#include "Logger.hpp"
#include "communication/CommDefs.hpp"
#include "TlsWriteQueue.h"

namespace bft::communication {

class TlsTcpImpl;

typedef boost::asio::ssl::stream<boost::asio::ip::tcp::socket> SSL_SOCKET;

class AsyncTlsConnection : public std::enable_shared_from_this<AsyncTlsConnection> {
 public:
  static constexpr size_t MSG_HEADER_SIZE = 4;
  static constexpr std::chrono::seconds READ_TIMEOUT = std::chrono::seconds(10);
  static constexpr std::chrono::seconds WRITE_TIMEOUT = READ_TIMEOUT;

  // We require a factory function because we can't call shared_from_this() in the constructor.
  //
  // In order to call shared_from_this(), there must already be a shared pointer wrapping `this`.
  // In this case, shared_from_this() is required for registering verification callbacks in the SSL context
  // initialization functions.
  static std::shared_ptr<AsyncTlsConnection> create(boost::asio::io_service& io_service,
                                                    boost::asio::ip::tcp::socket&& socket,
                                                    IReceiver* receiver,
                                                    TlsTCPCommunication::TlsTcpImpl& impl,
                                                    size_t max_buffer_size) {
    auto conn = std::make_shared<AsyncTlsConnection>(io_service, receiver, impl, max_buffer_size);
    conn->initServerSSLContext();
    conn->createSSLSocket(std::move(socket));
    return conn;
  }

  static std::shared_ptr<AsyncTlsConnection> create(boost::asio::io_service& io_service,
                                                    boost::asio::ip::tcp::socket&& socket,
                                                    IReceiver* receiver,
                                                    TlsTCPCommunication::TlsTcpImpl& impl,
                                                    size_t max_buffer_size,
                                                    NodeNum destination) {
    auto conn = std::make_shared<AsyncTlsConnection>(io_service, receiver, impl, max_buffer_size, destination);
    conn->initClientSSLContext(destination);
    conn->createSSLSocket(std::move(socket));
    return conn;
  }

  // Constructor for an accepting (server) connection.
  AsyncTlsConnection(boost::asio::io_service& io_service,
                     IReceiver* receiver,
                     TlsTCPCommunication::TlsTcpImpl& impl,
                     size_t max_buffer_size)
      : logger_(logging::getLogger("concord-bft.tls")),
        io_service_(io_service),
        ssl_context_(boost::asio::ssl::context::tlsv12_server),
        receiver_(receiver),
        tlsTcpImpl_(impl),
        read_timer_(io_service_),
        write_timer_(io_service_),
        read_msg_(max_buffer_size) {}

  // Constructor for a connecting (client) connection.
  AsyncTlsConnection(boost::asio::io_service& io_service,
                     IReceiver* receiver,
                     TlsTCPCommunication::TlsTcpImpl& impl,
                     size_t max_buffer_size,
                     NodeNum peer_id)
      : logger_(logging::getLogger("concord-bft.tls")),
        io_service_(io_service),
        ssl_context_(boost::asio::ssl::context::tlsv12_client),
        peer_id_(peer_id),
        receiver_(receiver),
        tlsTcpImpl_(impl),
        read_timer_(io_service_),
        write_timer_(io_service_),
        read_msg_(max_buffer_size) {}

  void setPeerId(NodeNum peer_id) { peer_id_ = peer_id; }
  std::optional<NodeNum> getPeerId() { return peer_id_; }
  SSL_SOCKET& getSocket() { return *socket_.get(); }

  // Every messsage is preceded by a 4 byte message header that we must read.
  // `bytes_already_read` == `std::nullopt` if this is the first read call.
  void readMsgSizeHeader();
  void readMsgSizeHeader(std::optional<size_t> bytes_already_read);

  // This should only be called in the io thread.
  void write();

  // The write queue is set after the connection is authenticated.
  void setWriteQueue(WriteQueue* queue) {
    ConcordAssert(write_queue_ == nullptr);
    write_queue_ = queue;
  }

  // Clean up the connection
  void dispose(bool close_connection = true);

 private:
  // We know the size of the message and that a message should be forthcoming. We start a timer and
  // ensure we read all remaining bytes within a given timeout. If we read the full message we
  // inform the `receiver_`, otherwise we tell the `tlsTcpImpl` that the connection should be
  // removed and closed.
  void readMsg();

  // Return the recently read size header as an integer. Assume network byte order.
  uint32_t getReadMsgSize();

  void startReadTimer();
  void startWriteTimer();

  void createSSLSocket(boost::asio::ip::tcp::socket&&);
  void initClientSSLContext(NodeNum destination);
  void initServerSSLContext();

  // Callbacks triggered from asio for certificate validation
  // On the server side we don't know the identity of the accepted connection until we verify the certificate and read
  // the node id.
  bool verifyCertificateClient(bool preverified, boost::asio::ssl::verify_context& ctx, NodeNum expected_dest_id);
  bool verifyCertificateServer(bool preverified, boost::asio::ssl::verify_context& ctx);

  // Certificate pinning
  //
  // Check for a specific certificate and do not rely on the chain authentication.
  //
  // Return true along with the actual node id if verification succeeds, (false, 0) if not.
  std::pair<bool, NodeNum> checkCertificate(X509* cert,
                                            std::string connectionType,
                                            std::string subject,
                                            std::optional<NodeNum> expected_peer_id);

  logging::Logger logger_;
  boost::asio::io_service& io_service_;

  // We can't rely on timer callbacks to actually be cancelled properly and return
  // `boost::asio::error::operation_aborted`. There is a race condition where a callback may already
  // be queued before the timer was cancelled. If we destroy the socket due to a failed write, and
  // then the callback fires, we may have already closed the socket, and we don't want to do that
  // twice.
  bool disposed_ = false;

  boost::asio::ssl::context ssl_context_;

  std::unique_ptr<SSL_SOCKET> socket_;
  std::optional<NodeNum> peer_id_ = std::nullopt;

  // We assume `receiver_` lives at least as long as each connection.
  IReceiver* receiver_ = nullptr;

  // We assume `tlsTcpImpl_` lives at least as long as each connection
  TlsTCPCommunication::TlsTcpImpl& tlsTcpImpl_;

  boost::asio::steady_timer read_timer_;
  boost::asio::steady_timer write_timer_;

  // On every read, we must read the size of the incoming message first. This buffer stores that size.
  std::array<char, MSG_HEADER_SIZE> read_size_buf_;

  // Last read message
  std::vector<char> read_msg_;

  // Message being currently written.
  std::optional<OutgoingMsg> write_msg_;

  WriteQueue* write_queue_ = nullptr;
};

}  // namespace bft::communication
