// Concord
//
// Copyright (c) 2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use
// this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license
// terms. Your use of these subcomponents is subject to the terms and conditions of the
// subcomponent's license, as noted in the LICENSE file.

#include <asio.hpp>
#include <atomic>
#include <thread>

#include "communication/CommDefs.hpp"
#include "Logger.hpp"
#include "TlsWriteQueue.h"

#pragma once

namespace bft::communication::tls {

// Forward declaration
class AsyncTlsConnection;

// Manage all connections for a client or server in a peer-to-peer model.
//
// Peers with higher ids connect to peers with lower ids.
class ConnectionManager {
  static constexpr size_t MSG_HEADER_SIZE = 4;
  static constexpr std::chrono::seconds CONNECT_TICK = std::chrono::seconds(1);
  friend class Runner;
  friend class AsyncTlsConnection;

 public:
  ConnectionManager(const TlsTcpConfig &, asio::io_context &);

  //
  // Methods required by ICommuncication
  // Called as part of pImpl idiom
  //
  ConnectionStatus getCurrentConnectionStatus(const NodeNum node) const;
  void setReceiver(NodeNum nodeId, IReceiver *receiver);
  void send(const NodeNum destination, const std::shared_ptr<OutgoingMsg> &msg);
  void send(const std::set<NodeNum> &destinations, const std::shared_ptr<OutgoingMsg> &msg);
  int getMaxMessageSize() const;
  void dispose(NodeNum i);
 private:
  void start();
  void stop();

  // Open a socket, configure it, bind it, and start listening on a given host/pot.
  void listen();

  // Start asynchronously accepting connections.
  void accept();

  // Perform synhronous DNS resolution. This really only works for the listen socket, since after that we want to do a
  // new lookup on every connect operation in case the underlying IP of the DNS address changes.
  //
  // Throws an asio::system_error if it fails to resolve
  asio::ip::tcp::endpoint syncResolve();

  // Starts async dns resolution on a tcp socket.
  //
  // Every call to connect will call resolve before asio async_connect call.
  void resolve(NodeNum destination);

  // Connet to other nodes where there is not a current connection.
  //
  // Replicas only connect to replicas with smaller node ids.
  // This method is called at startup and also on a periodic timer tick.
  void connect();
  void connect(NodeNum, const asio::ip::tcp::endpoint &);

  // Start a steady_timer in order to trigger needed `connect` operations.
  void startConnectTimer();

  // Trigger the asio async_hanshake calls.
  void startServerSSLHandshake(asio::ip::tcp::socket &&);
  void startClientSSLHandshake(asio::ip::tcp::socket &&socket, NodeNum destination);

  // Callbacks triggered when asio async_hanshake completes for an incoming or outgoing connection.
  void onServerHandshakeComplete(const asio::error_code &ec, size_t accepted_connection_id);
  void onClientHandshakeComplete(const asio::error_code &ec, NodeNum destination);

  // If onServerHandshake completed successfully, this function will get called and add the AsyncTlsConnection to
  // `connections_`.
  void onConnectionAuthenticated(std::shared_ptr<AsyncTlsConnection> conn);

  // Synchronously close connections
  // This is only used during shutdown after the io_service is stopped.
  void syncCloseConnection(std::shared_ptr<AsyncTlsConnection> &conn);

  // The connection itself has determined it needs to be shutdown. Since it runs in a separate strand from the ConnMgr,
  // it most post to the ConnMgr via this method.
  void remoteCloseConnection(NodeNum);

  // Asynchronously shutdown an SSL connection and then close the underlying TCP socket when the shutdown has
  // completed.
  void closeConnection(std::shared_ptr<AsyncTlsConnection> conn);

  bool isReplica() const { return isReplica(config_.selfId); }
  bool isReplica(NodeNum id) const { return id <= static_cast<size_t>(config_.maxServerId); }

  // Sends from other threads in the system are posted to strand_. This callback gets run in the
  // strand so it can lookup a connection and send it a message. This strategy avoids the need to
  // take a lock over connections_.
  void handleSend(const NodeNum destination, std::shared_ptr<OutgoingMsg> msg);
  void handleSend(const std::set<NodeNum> &destinations, const std::shared_ptr<OutgoingMsg> &msg);

  // Answer connection status requests from other threads
  // Returns true in the promise if the destination is connected, false otherwise.
  void handleConnStatus(const NodeNum destination, std::promise<bool> &connected) const;

 private:
  logging::Logger logger_;
  TlsTcpConfig config_;

  // The lifetime of the receiver must be at least as long as the lifetime of this object
  IReceiver *receiver_;

  // For each accepted connection, we bump this value. We use it as a key into a map to find any in
  // progress connections when cert validation completes
  size_t total_accepted_connections_ = 0;

  asio::io_context &io_context_;
  asio::strand<asio::io_context::executor_type> strand_;
  asio::ip::tcp::acceptor acceptor_;
  asio::ip::tcp::resolver resolver_;
  asio::steady_timer connect_timer_;

  // This tracks outstanding attempts at DNS resolution for outgoing connections.
  std::set<NodeNum> resolving_;

  // Sockets that are in progress of connecting.
  // When these connections complete, an AsyncTlsConnection will be created and moved into
  // `connected_waiting_for_handshake_`.
  std::unordered_map<NodeNum, asio::ip::tcp::socket> connecting_;

  // Connections that are in progress of waiting for a handshake to complete.
  // When the handshake completes these will be moved into `connections_`.
  std::unordered_map<NodeNum, std::shared_ptr<AsyncTlsConnection>> connected_waiting_for_handshake_;

  // Connections that have been accepted, but where the handshake has not been completed.
  // When the handshake completes these will be moved into `connections_`.
  std::unordered_map<size_t, std::shared_ptr<AsyncTlsConnection>> accepted_waiting_for_handshake_;

  // Active, secured connections.
  std::unordered_map<NodeNum, std::shared_ptr<AsyncTlsConnection>> connections_;

  // Diagnostics
  std::shared_ptr<TlsStatus> status_;
  Recorders histograms_;
};

}  // namespace bft::communication::tls
