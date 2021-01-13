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

#include <atomic>
#include <chrono>
#include <mutex>
#include <thread>
#include <unordered_set>

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/asio/steady_timer.hpp>

#include "communication/CommDefs.hpp"
#include "Logger.hpp"
#include "AsyncTlsConnection.h"
#include "TlsDiagnostics.h"
#include "TlsWriteQueue.h"

#pragma once

namespace bft::communication {

typedef boost::asio::ssl::stream<boost::asio::ip::tcp::socket> SSL_SOCKET;

/******************************************************************************
 * Implementation class. Is reponsible for creating listener on given port,
 * outgoing connections to the lower Id peers and accepting connections from
 *  higher ID peers.
 *
 *  This is default behavior given the clients will always have higher IDs
 *  from the replicas. In this way we assure that clients will not connect to
 *  each other.
 *
 *  We will have to revisit this connection strategy once we have dynamic reconfiguration.
 *
 * ***************************
 * Threading Model
 * ***************************
 *
 * This implementation is based around [boost::asio](https://think-async.com/Asio/), which uses
 * non-blocking sockets and an event driven callback model. It uses an older version of boost
 * (1.65) which when upgraded will require some small changes to the code base, such as
 * `io_service` becoming `io_context`.
 *
 * To simplify the implementation, we only run a single io service thread. This thread handles all
 * connecting, accepting, sending, receiving, and timers for sockets.
 *
 * The current implementation maps a single `principle` (concord-bft replica or bft-client) to a
 * single ICommunication interface, backed by a single ASIO thread. All connections to and from
 * this principle are managed by this ASIO thread. ASIO is able to support much higher levels of
 * concurrency, and this behavior is currently an artificial limit, although it also simplifies the
 * implementation.
 *
 * Since all callbacks for asynchronous behavior occur on a single ASIO service thread, no mutexes
 * are required to protect data only touched by that thread. Most of the data managed by the
 * TlsTcpImpl and AsyncTlsConnection classes falls into this category. There are, however, two
 * scenarios (outside of diagnostics), where data must be made thread-safe, since the operations
 * come from other threads:
 *     1. Putting messages on a queue to be sent by the ASIO thread
 *     2. Queries for the current connection status
 *
 *  (1) occurs when the `ICommunication::SendAsyncMsg` method is called.
 *  (2) occurs when the `ICommunication::getCurrentConnectionStatus` method is called.
 *
 * **************************
 * Notes on Performance
 * **************************
 *
 * Copying
 * ----------------
 * The ICommunication and IReceiver interfaces' use of raw pointers in `sendAsyncMessage` and
 * `onNewMessage`,respeectively, force the callee to copy the data. This copying occurs once for
 * every message sent or received over a single socket. Compounding matters, the SBFT protocol
 * itself often sends identical messages to all replicas, thus forcing an additional copy before
 * the `sendAsyncMessage` method is called.
 *
 * It is strongly recommended that the ICommunication interface be changed to prevent this
 * unnecessary copying. Additionally, value types should be used to not only reduce copying, but
 * simplify the memory management/ownership model. One implementation choice would be to take
 * `std::vector<uint8_t>` rather than raw pointer/length pairs taken currently for both
 * `sendAsyncMessage` and `onNewMessage`. Additionally, a new `broadcast` call can be created that
 * passes a shared message buffer, such as a `Sliver` and a list of destinations, such that zero
 * copying is required to get N replicated messages into the IO thread.
 *
 * Thread Safety
 * ----------------
 * Currently all sockets are protected by a single mutex, `connections_guard_`. Since only a single
 * replica or client thread interacts with the IO thread, this probably works fine, as contention
 * is naturally limited. It's worth noting, however, that if more than one thread can send to
 * different endpoints, we may hit more contention, where more fine grained locks, or lockfree
 * strategies may make sense. This codebase is instrumted with histograms to enable discovery of
 * issues during both testing and production.
 *
 *****************************************************************************/
class TlsTCPCommunication::TlsTcpImpl {
  static constexpr size_t LISTEN_BACKLOG = 5;
  static constexpr std::chrono::seconds CONNECT_TICK = std::chrono::seconds(1);

  friend class AsyncTlsConnection;

 public:
  static std::unique_ptr<TlsTcpImpl> create(const TlsTcpConfig &config) { return std::make_unique<TlsTcpImpl>(config); }

  TlsTcpImpl(const TlsTcpConfig &config)
      : logger_(logging::getLogger("concord-bft.tls")),
        config_(config),
        acceptor_(io_service_),
        resolver_(io_service_),
        accepting_socket_(io_service_),
        connect_timer_(io_service_),
        status_(std::make_shared<TlsStatus>()),
        histograms_(Recorders(std::to_string(config.selfId), config.bufferLength, MAX_QUEUE_SIZE_IN_BYTES)) {
    auto &registrar = concord::diagnostics::RegistrarSingleton::getInstance();
    concord::diagnostics::StatusHandler handler(
        "tls" + std::to_string(config.selfId), "TlsTcpImpl status", [this]() { return status_->status(); });
    registrar.status.registerHandler(handler);
    write_queues_.reserve(config_.nodes.size() - 1);
    for (const auto node : config_.nodes) {
      if (node.first != config_.selfId) {
        NodeNum id = node.first;
        write_queues_.try_emplace(id, id, histograms_);
      }
    }
  }

  //
  // Methods required by ICommuncication
  // Called as part of pImpl idiom
  //
  int Start();
  int Stop();
  bool isRunning() const;
  ConnectionStatus getCurrentConnectionStatus(const NodeNum node) const;
  void setReceiver(NodeNum nodeId, IReceiver *receiver);
  int sendAsyncMessage(const NodeNum destination, const char *const msg, const size_t msg_len);
  int getMaxMessageSize();

 private:
  // Open a socket, configure it, bind it, and start listening on a given host/pot.
  void listen();

  // Start asynchronously accepting connections.
  void accept();

  // Perform synhronous DNS resolution. This really only works for the listen socket, since after that we want to do a
  // new lookup on every connect operation in case the underlying IP of the DNS address changes.
  //
  // Throws a boost::system::system_error if it fails to resolve
  boost::asio::ip::tcp::endpoint syncResolve();

  // Starts async dns resolution on a tcp socket.
  //
  // Every call to connect will call resolve before asio async_connect call.
  void resolve(NodeNum destination);

  // Connet to other nodes where there is not a current connection.
  //
  // Replicas only connect to replicas with smaller node ids.
  // This method is called at startup and also on a periodic timer tick.
  void connect();
  void connect(NodeNum, boost::asio::ip::tcp::endpoint);

  // Start a steady_timer in order to trigger needed `connect` operations.
  void startConnectTimer();

  // Trigger the asio async_hanshake calls.
  void startServerSSLHandshake(boost::asio::ip::tcp::socket &&);
  void startClientSSLHandshake(boost::asio::ip::tcp::socket &&socket, NodeNum destination);

  // Callbacks triggered when asio async_hanshake completes for an incoming or outgoing connection.
  void onServerHandshakeComplete(const boost::system::error_code &ec, size_t accepted_connection_id);
  void onClientHandshakeComplete(const boost::system::error_code &ec, NodeNum destination);

  // If onServerHandshake completed successfully, this function will get called and add the AsyncTlsConnection to
  // `connections_`.
  void onConnectionAuthenticated(std::shared_ptr<AsyncTlsConnection> conn);

  // Synchronously close connections
  // This is only used during shutdown after the io_service is stopped.
  void syncCloseConnection(std::shared_ptr<AsyncTlsConnection> &conn);

  // Asynchronously shutdown an SSL connection and then close the underlying TCP socket when the shutdown has
  // completed.
  void closeConnection(NodeNum);
  void closeConnection(std::shared_ptr<AsyncTlsConnection> conn);

  bool isReplica() { return isReplica(config_.selfId); }
  bool isReplica(NodeNum id) { return id <= static_cast<size_t>(config_.maxServerId); }

  logging::Logger logger_;
  TlsTcpConfig config_;

  // The lifetime of the receiver must be at least as long as the lifetime of this object
  IReceiver *receiver_;

  // This thread runs the asio io_service. It's the main thread of the TlsTcpImpl.
  std::unique_ptr<std::thread> io_thread_;

  // io_thread_ lifecycle management
  mutable std::mutex io_thread_guard_;

  // Use io_context when we upgrade boost, as io_service is deprecated
  // https://stackoverflow.com/questions/59753391/boost-asio-io-service-vs-io-context
  boost::asio::io_service io_service_;
  boost::asio::ip::tcp::acceptor acceptor_;

  // We store a single resolver so that it doesn't go out of scope during async_resolve calls.
  boost::asio::ip::tcp::resolver resolver_;

  // Every async_accept call for asio requires us to pass it an existing socket to write into. Later versions do not
  // require this. This socket will be filled in by an accepted connection. We'll then move it into an
  // AsyncTlsConnection when the async_accept handler returns.
  boost::asio::ip::tcp::socket accepting_socket_;

  // For each accepted connection, we bump this value. We use it as a key into a map to find any in
  // progress connections when cert validation completes
  size_t total_accepted_connections_ = 0;

  // This timer is called periodically to trigger connections as needed.
  boost::asio::steady_timer connect_timer_;

  // This tracks outstanding attempts at DNS resolution for outgoing connections.
  std::set<NodeNum> resolving_;

  // Sockets that are in progress of connecting.
  // When these connections complete, an AsyncTlsConnection will be created and moved into
  // `connected_waiting_for_handshake_`.
  std::unordered_map<NodeNum, boost::asio::ip::tcp::socket> connecting_;

  // Connections that are in progress of waiting for a handshake to complete.
  // When the handshake completes these will be moved into `connections_`.
  std::unordered_map<NodeNum, std::shared_ptr<AsyncTlsConnection>> connected_waiting_for_handshake_;

  // Connections that have been accepted, but where the handshake has not been completed.
  // When the handshake completes these will be moved into `connections_`.
  std::unordered_map<size_t, std::shared_ptr<AsyncTlsConnection>> accepted_waiting_for_handshake_;

  // Connections are manipulated from multiple threads. The io_service thread creates them and runs callbacks on them.
  // Senders find a connection through this map and push data onto the outQueue.
  mutable std::mutex connections_guard_;
  std::unordered_map<NodeNum, std::shared_ptr<AsyncTlsConnection>> connections_;

  // This is a cache of the connected nodes in `connections_`. It's used to determine who to try to connect to in the
  // connect callback in the io thread so that `connections_guard` does not have to be locked. We only need to lock
  // `connections_guard` in the io thread when a connection is authenticated or disposed.
  std::unordered_set<NodeNum> active_connections_;

  // Each destination has its own WriteQueue. The lifetime of these queues is the lifetime of the
  // TlsTcpImpl. When a connection is established for that destination, a reference to the write
  // queue is passed into the conneciton.
  std::unordered_map<NodeNum, WriteQueue> write_queues_;

  // Diagnostics
  std::shared_ptr<TlsStatus> status_;
  Recorders histograms_;
};

}  // namespace bft::communication
