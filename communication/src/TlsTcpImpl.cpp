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

#include <boost/filesystem.hpp>

#include "assertUtils.hpp"
#include "TlsTcpImpl.h"

using concord::diagnostics::TimeRecorder;

namespace bft::communication {

int TlsTCPCommunication::TlsTcpImpl::Start() {
  std::lock_guard<std::mutex> l(io_thread_guard_);
  if (io_thread_) return 0;

  // Start the io_thread_;
  io_thread_.reset(new std::thread([this]() {
    LOG_INFO(logger_, "io thread starting: isReplica() = " << isReplica());
    if (isReplica()) {
      listen();
      accept();
    }
    for (auto i = 0; i <= config_.maxServerId; i++) {
      if (config_.statusCallback) {
        auto node = config_.nodes.at(i);
        PeerConnectivityStatus pcs{};
        pcs.peerId = i;
        pcs.peerHost = node.host;
        pcs.peerPort = node.port;
        pcs.statusType = StatusType::Started;
        config_.statusCallback(pcs);
      }
    }
    connect();
    startConnectTimer();

    // We must start connecting and accepting before we start the io_service, so that it has some
    // work to do. This is what prevents the io_service event loop from exiting immediately.
    io_service_.run();
  }));
  return 0;
}

int TlsTCPCommunication::TlsTcpImpl::Stop() {
  status_->reset();
  std::lock_guard<std::mutex> l(io_thread_guard_);
  if (!io_thread_) return 0;
  io_service_.stop();
  if (io_thread_->joinable()) {
    io_thread_->join();
    io_thread_.reset(nullptr);
  }

  acceptor_.close();
  accepting_socket_.close();
  for (auto& [_, sock] : connecting_) {
    (void)_;  // unused variable hack
    sock.close();
  }
  for (auto& [_, conn] : connected_waiting_for_handshake_) {
    (void)_;  // unused variable hack
    syncCloseConnection(conn);
  }

  for (auto& [_, conn] : accepted_waiting_for_handshake_) {
    (void)_;  // unused variable hack
    syncCloseConnection(conn);
  }

  for (auto& [_, conn] : connections_) {
    (void)_;  // unused variable hack
    syncCloseConnection(conn);
  }

  for (auto& [_, write_queue] : write_queues_) {
    (void)_;  // unused variable hack
    write_queue.clear();
  }

  return 0;
}

void TlsTCPCommunication::TlsTcpImpl::startConnectTimer() {
  connect_timer_.expires_from_now(CONNECT_TICK);
  connect_timer_.async_wait([this](const boost::system::error_code& ec) {
    if (ec) {
      if (ec == boost::asio::error::operation_aborted) {
        // We are shutting down the system. Just return.
        return;
      }
      LOG_FATAL(logger_, "Connect timer wait failure: " << ec.message());
      abort();
    }
    connect();
    startConnectTimer();
  });
}

void TlsTCPCommunication::TlsTcpImpl::listen() {
  try {
    auto endpoint = syncResolve();
    acceptor_.open(endpoint.protocol());
    acceptor_.set_option(boost::asio::socket_base::reuse_address(true));
    acceptor_.bind(endpoint);
    acceptor_.listen(LISTEN_BACKLOG);
    LOG_INFO(logger_, "TLS server listening at " << endpoint);
  } catch (const boost::system::system_error& e) {
    LOG_FATAL(logger_,
              "Faield to start TlsTCPImpl acceptor at " << config_.listenHost << ":" << config_.listenPort
                                                        << " for node " << config_.selfId << ": " << e.what());
    abort();
  }
}

bool TlsTCPCommunication::TlsTcpImpl::isRunning() const {
  std::lock_guard<std::mutex> l(io_thread_guard_);
  return io_thread_.get() != nullptr;
}

void TlsTCPCommunication::TlsTcpImpl::setReceiver(NodeNum _, IReceiver* receiver) {
  // We don't allow setting a receiver after startup
  ConcordAssert(!isRunning());
  receiver_ = receiver;
}

int TlsTCPCommunication::TlsTcpImpl::sendAsyncMessage(const NodeNum destination, const char* msg, const size_t len) {
  auto max_size = config_.bufferLength - AsyncTlsConnection::MSG_HEADER_SIZE;
  if (len > max_size) {
    status_->total_messages_dropped++;
    LOG_ERROR(logger_, "Msg Dropped. Size exceeds max message size: " << KVLOG(len, max_size));
    return -1;
  }

  auto& queue = write_queues_.at(destination);
  auto queue_size_after_push = queue.push(msg, len);
  if (!queue_size_after_push) {
    LOG_DEBUG(logger_, "Connection NOT found or queue full, from: " << config_.selfId << ", to: " << destination);
    status_->total_messages_dropped++;
    return -1;
  }

  auto conn = queue.getConn();

  // If queue_size_after_push > 1 then the io_thread is already writing. It's somewhat expensive to
  // call `io_serivce.post`, and so we only write if we know there's a good chance no other write is
  // going on. Note that due to the fact that the current message being sent is popped off the write
  // queue, it's possible there's an inflight write going on and this thread doesn't know about it.
  // Therefore, we may call post every time a message is sent if the queue never grows beyond 1.
  if (queue_size_after_push.value() == 1) {
    if (conn) {
      io_service_.post([conn]() { conn->write(); });
    }
  }

  if (conn) {
    LOG_DEBUG(logger_, "Sent message from: " << config_.selfId << ", to: " << destination << "with size: " << len);
    status_->total_messages_sent++;

    if (config_.statusCallback && isReplica()) {
      PeerConnectivityStatus pcs{};
      pcs.peerId = config_.selfId;
      pcs.statusType = StatusType::MessageSent;
      config_.statusCallback(pcs);
    }
    return 0;
  } else {
    status_->total_messages_dropped++;
    return -1;
  }
}

void setSocketOptions(boost::asio::ip::tcp::socket& socket) { socket.set_option(boost::asio::ip::tcp::no_delay(true)); }

void TlsTCPCommunication::TlsTcpImpl::closeConnection(NodeNum id) {
  LOG_INFO(logger_, "Closing connection from: " << config_.selfId << ", to: " << id);
  std::lock_guard<std::mutex> lock(connections_guard_);
  auto conn = std::move(connections_.at(id));
  connections_.erase(id);
  active_connections_.erase(id);
  write_queues_.at(id).disconnect();
  status_->num_connections = connections_.size();
  if (config_.statusCallback && isReplica(id)) {
    PeerConnectivityStatus pcs{};
    pcs.peerId = id;
    pcs.statusType = StatusType::Broken;
    config_.statusCallback(pcs);
  }
  closeConnection(std::move(conn));
}

void TlsTCPCommunication::TlsTcpImpl::closeConnection(std::shared_ptr<AsyncTlsConnection> conn) {
  // We don't want AsyncTlsConnection to close the socket, since we are doing that here.
  static constexpr bool close_connection = false;
  conn->dispose(close_connection);
  conn->getSocket().lowest_layer().cancel();
  conn->getSocket().async_shutdown([this, conn](const auto& ec) {
    if (ec) {
      LOG_WARN(logger_, "SSL shutdown failed: " << ec.message());
    }
    conn->getSocket().lowest_layer().close();
  });
}

void TlsTCPCommunication::TlsTcpImpl::syncCloseConnection(std::shared_ptr<AsyncTlsConnection>& conn) {
  boost::system::error_code _;
  conn->getSocket().shutdown(_);
  conn->getSocket().lowest_layer().close();
}

void TlsTCPCommunication::TlsTcpImpl::onConnectionAuthenticated(std::shared_ptr<AsyncTlsConnection> conn) {
  // Move the connection into the accepted connections map. If there is an existing connection
  // discard it. In this case it was likely that connecting end of the connection thinks there is
  // something wrong. This is a vector for a denial of service attack on the accepting side. We can
  // track the number of connections from the node and mark it malicious if necessary.
  TimeRecorder scoped_timer(*histograms_.on_connection_authenticated);
  auto& queue = write_queues_.at(conn->getPeerId().value());
  {
    std::lock_guard<std::mutex> lock(connections_guard_);
    auto it = connections_.find(conn->getPeerId().value());
    if (it != connections_.end()) {
      LOG_INFO(logger_,
               "New connection accepted from same peer. Closing existing connection to " << conn->getPeerId().value());
      closeConnection(std::move(it->second));
    }
    conn->setWriteQueue(&queue);
    connections_.insert_or_assign(conn->getPeerId().value(), conn);
    status_->num_connections = connections_.size();
  }
  queue.connect(conn);
  active_connections_.insert(conn->getPeerId().value());
  conn->readMsgSizeHeader();
}

void TlsTCPCommunication::TlsTcpImpl::onServerHandshakeComplete(const boost::system::error_code& ec,
                                                                size_t accepted_connection_id) {
  auto conn = std::move(accepted_waiting_for_handshake_.at(accepted_connection_id));
  accepted_waiting_for_handshake_.erase(accepted_connection_id);
  status_->num_accepted_waiting_for_handshake = accepted_waiting_for_handshake_.size();
  if (ec) {
    auto peer_str = conn->getPeerId().has_value() ? std::to_string(conn->getPeerId().value()) : "Unknown";
    LOG_ERROR(logger_, "Server handshake failed for peer " << peer_str << ": " << ec.message());
    return closeConnection(std::move(conn));
  }
  LOG_INFO(logger_, "Server handshake succeeded for peer " << conn->getPeerId().value());
  onConnectionAuthenticated(std::move(conn));
}

void TlsTCPCommunication::TlsTcpImpl::onClientHandshakeComplete(const boost::system::error_code& ec,
                                                                NodeNum destination) {
  auto conn = std::move(connected_waiting_for_handshake_.at(destination));
  connected_waiting_for_handshake_.erase(destination);
  status_->num_connected_waiting_for_handshake = connected_waiting_for_handshake_.size();
  if (ec) {
    LOG_ERROR(logger_, "Client handshake failed for peer " << conn->getPeerId().value() << ": " << ec.message());
    return closeConnection(std::move(conn));
  }
  status_->total_connect_attempts_completed++;
  LOG_INFO(logger_, "Client handshake succeeded for peer " << conn->getPeerId().value());
  onConnectionAuthenticated(std::move(conn));
}

void TlsTCPCommunication::TlsTcpImpl::startServerSSLHandshake(boost::asio::ip::tcp::socket&& socket) {
  auto connection_id = total_accepted_connections_;
  auto conn = AsyncTlsConnection::create(io_service_, std::move(socket), receiver_, *this, config_.bufferLength);
  accepted_waiting_for_handshake_.insert({connection_id, conn});
  status_->num_accepted_waiting_for_handshake = accepted_waiting_for_handshake_.size();
  conn->getSocket().async_handshake(
      boost::asio::ssl::stream_base::server,
      [this, connection_id](const boost::system::error_code& ec) { onServerHandshakeComplete(ec, connection_id); });
}

void TlsTCPCommunication::TlsTcpImpl::startClientSSLHandshake(boost::asio::ip::tcp::socket&& socket,
                                                              NodeNum destination) {
  auto conn =
      AsyncTlsConnection::create(io_service_, std::move(socket), receiver_, *this, config_.bufferLength, destination);
  connected_waiting_for_handshake_.insert({destination, conn});
  status_->num_connected_waiting_for_handshake = connected_waiting_for_handshake_.size();
  conn->getSocket().async_handshake(
      boost::asio::ssl::stream_base::client,
      [this, destination](const boost::system::error_code& ec) { onClientHandshakeComplete(ec, destination); });
}

void TlsTCPCommunication::TlsTcpImpl::accept() {
  acceptor_.async_accept(accepting_socket_, [this](boost::system::error_code ec) {
    if (ec) {
      LOG_WARN(logger_, "async_accept failed: " << ec.message());
      // When io_service is stopped, the handlers are destroyed and when the
      // io_service dtor runs they will be invoked with operation_aborted error.
      // In this case we dont want to accept again.
      if (ec == boost::asio::error::operation_aborted) return;
    }
    total_accepted_connections_++;
    status_->total_accepted_connections = total_accepted_connections_;
    setSocketOptions(accepting_socket_);
    LOG_INFO(logger_, "Accepted connection " << total_accepted_connections_);
    startServerSSLHandshake(std::move(accepting_socket_));
    accept();
  });
}

void TlsTCPCommunication::TlsTcpImpl::resolve(NodeNum i) {
  resolving_.insert(i);
  status_->num_resolving = resolving_.size();
  auto node = config_.nodes.at(i);
  boost::asio::ip::tcp::resolver::query query(boost::asio::ip::tcp::v4(), node.host, std::to_string(node.port));
  resolver_.async_resolve(query, [this, node, i, query](const auto& error_code, auto results) {
    if (error_code) {
      LOG_WARN(
          logger_,
          "Failed to resolve node " << i << ": " << node.host << ":" << node.port << " : " << error_code.message());
      resolving_.erase(i);
      status_->num_resolving = resolving_.size();
      return;
    }
    boost::asio::ip::tcp::endpoint endpoint = *results;
    LOG_INFO(logger_, "Resolved node " << i << ": " << node.host << ":" << node.port << " to " << endpoint);
    resolving_.erase(i);
    status_->num_resolving = resolving_.size();
    connect(i, endpoint);
  });
}

void TlsTCPCommunication::TlsTcpImpl::connect(NodeNum i, boost::asio::ip::tcp::endpoint endpoint) {
  auto [it, inserted] = connecting_.emplace(i, boost::asio::ip::tcp::socket(io_service_));
  ConcordAssert(inserted);
  status_->num_connecting = connecting_.size();
  it->second.async_connect(endpoint, [this, i, endpoint](const auto& error_code) {
    if (error_code) {
      LOG_WARN(logger_, "Failed to connect to node " << i << ": " << endpoint);
      connecting_.at(i).close();
      connecting_.erase(i);
      status_->num_connecting = connecting_.size();
      return;
    }
    LOG_INFO(logger_, "Connected to node " << i << ": " << endpoint);
    auto connected_socket = std::move(connecting_.at(i));
    connecting_.erase(i);
    status_->num_connecting = connecting_.size();
    startClientSSLHandshake(std::move(connected_socket), i);
  });
}

void TlsTCPCommunication::TlsTcpImpl::connect() {
  std::lock_guard<std::mutex> lock(connections_guard_);
  auto end = std::min<size_t>(config_.selfId, config_.maxServerId + 1);
  for (auto i = 0u; i < end; i++) {
    if (connections_.count(i) == 0 && connecting_.count(i) == 0 && resolving_.count(i) == 0 &&
        connected_waiting_for_handshake_.count(i) == 0) {
      resolve(i);
    }
  }
}

boost::asio::ip::tcp::endpoint TlsTCPCommunication::TlsTcpImpl::syncResolve() {
  // TODO: When upgrading to boost 1.66 or later, when query is deprecated,
  // this should be changed to call the resolver.resolve overload that takes a
  // u, host, and service directly, instead of a query object. That
  // overload is not yet available in boost 1.64, which we're using today.
  boost::asio::ip::tcp::resolver::query query(
      boost::asio::ip::tcp::v4(), config_.listenHost, std::to_string(config_.listenPort));
  boost::asio::ip::tcp::resolver::iterator results = resolver_.resolve(query);
  boost::asio::ip::tcp::endpoint endpoint = *results;
  LOG_INFO(logger_, "Resolved " << config_.listenHost << ":" << config_.listenPort << " to " << endpoint);
  return endpoint;
}

int TlsTCPCommunication::TlsTcpImpl::getMaxMessageSize() { return config_.bufferLength; }

ConnectionStatus TlsTCPCommunication::TlsTcpImpl::getCurrentConnectionStatus(const NodeNum id) const {
  std::lock_guard<std::mutex> lock(connections_guard_);
  if (connections_.count(id) == 1) {
    return ConnectionStatus::Connected;
  }
  return ConnectionStatus::Disconnected;
}

}  // namespace bft::communication
