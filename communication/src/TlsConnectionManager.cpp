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

#include <asio/bind_executor.hpp>
#include <boost/filesystem.hpp>
#include <cstdint>
#include <future>
#include <string>

#include "assertUtils.hpp"
#include "TlsConnectionManager.h"
#include "AsyncTlsConnection.h"
#include "communication/StateControl.hpp"

namespace bft::communication::tls {

void setSocketOptions(asio::ip::tcp::socket& socket) { socket.set_option(asio::ip::tcp::no_delay(true)); }

ConnectionManager::ConnectionManager(const TlsTcpConfig& config, asio::io_context& io_context)
    : logger_(logging::getLogger("concord-bft.tls.connMgr")),
      config_(config),
      io_context_(io_context),
      strand_(asio::make_strand(io_context_)),
      acceptor_(io_context_),
      resolver_(io_context_),
      connect_timer_(io_context_),
      status_(std::make_shared<TlsStatus>()),
      histograms_(Recorders(std::to_string(config.selfId), config.bufferLength, MAX_QUEUE_SIZE_IN_BYTES)) {
  auto& registrar = concord::diagnostics::RegistrarSingleton::getInstance();
  concord::diagnostics::StatusHandler handler(
      "tls" + std::to_string(config.selfId), "TLS status", [this]() { return status_->status(); });
  registrar.status.registerHandler(handler);
}

void ConnectionManager::start() {
  LOG_INFO(logger_, "Starting connection manager for " << config_.selfId);
  stopped_ = false;
  if (isReplica()) {
    listen();
    accept();
  }

  connect();
  startConnectTimer();
}

void ConnectionManager::stop() {
  LOG_INFO(logger_, "Stopping connection manager for " << config_.selfId);
  stopped_ = true;
  status_->reset();
  acceptor_.close();
  for (auto& [_, sock] : connecting_) {
    (void)_;  // unused variable hack
    sock.close();
  }
  for (auto& [id, conn] : connected_waiting_for_handshake_) {
    LOG_DEBUG(logger_, "Closing connection from: " << config_.selfId << ", to: " << id);
    syncCloseConnection(conn);
  }

  for (auto& [id, conn] : accepted_waiting_for_handshake_) {
    LOG_DEBUG(logger_, "Closing connection from: " << config_.selfId << ", to: " << id);
    syncCloseConnection(conn);
  }

  for (auto& [id, conn] : connections_) {
    LOG_DEBUG(logger_, "Closing connection from: " << config_.selfId << ", to: " << id);
    syncCloseConnection(conn);
  }
}

void ConnectionManager::setReceiver(NodeNum, IReceiver* receiver) { receiver_ = receiver; }

void ConnectionManager::listen() {
  try {
    auto endpoint = syncResolve();
    acceptor_.open(endpoint.protocol());
    acceptor_.set_option(asio::socket_base::reuse_address(true));
    acceptor_.bind(endpoint);
    acceptor_.listen();
    LOG_INFO(logger_, "TLS server listening at " << endpoint << " for replica " << config_.selfId);
  } catch (const asio::system_error& e) {
    LOG_FATAL(logger_,
              "Failed to start TLS acceptor at " << config_.listenHost << ":" << config_.listenPort << " for replica"
                                                 << config_.selfId << ": " << e.what());
    abort();
  }
}

void ConnectionManager::startConnectTimer() {
  connect_timer_.expires_from_now(CONNECT_TICK);
  connect_timer_.async_wait(asio::bind_executor(strand_, [this](const asio::error_code& ec) {
    if (stopped_) {
      return;
    }
    if (ec) {
      LOG_ERROR(logger_, "Connect timer wait failure: " << ec.message());
      return;
    }
    connect();
    startConnectTimer();
  }));
}  // namespace bft::communication::tls

void ConnectionManager::send(const NodeNum destination, const std::shared_ptr<OutgoingMsg>& msg) {
  auto max_size = config_.bufferLength - MSG_HEADER_SIZE;
  if (msg->payload_size() > max_size) {
    status_->total_messages_dropped++;
    LOG_ERROR(logger_, "Msg Dropped. Size exceeds max message size: " << KVLOG(msg->payload_size(), max_size));
    return;
  }
  {
    concord::diagnostics::TimeRecorder<true> scoped_timer(*histograms_.send_post_to_mgr);
    asio::post(strand_, [this, destination, msg]() { handleSend(destination, msg); });
  }
}

void ConnectionManager::send(const std::set<NodeNum>& destinations, const std::shared_ptr<OutgoingMsg>& msg) {
  auto max_size = config_.bufferLength - MSG_HEADER_SIZE;
  if (msg->payload_size() > max_size) {
    status_->total_messages_dropped++;
    LOG_ERROR(logger_, "Msg Dropped. Size exceeds max message size: " << KVLOG(msg->payload_size(), max_size));
    return;
  }
  {
    concord::diagnostics::TimeRecorder<true> scoped_timer(*histograms_.send_post_to_mgr);
    asio::post(strand_, [this, destinations, msg]() { handleSend(destinations, msg); });
  }
}

void ConnectionManager::handleSend(const NodeNum destination, std::shared_ptr<OutgoingMsg> msg) {
  auto it = connections_.find(destination);
  if (it != connections_.end()) {
    it->second->send(std::move(msg));
    status_->total_messages_sent++;
  } else {
    status_->total_messages_dropped++;
  }
}

void ConnectionManager::handleSend(const std::set<NodeNum>& destinations, const std::shared_ptr<OutgoingMsg>& msg) {
  for (auto destination : destinations) {
    auto it = connections_.find(destination);
    if (it != connections_.end()) {
      auto cheap_copy = msg;
      it->second->send(std::move(cheap_copy));
      status_->total_messages_sent++;
    } else {
      status_->total_messages_dropped++;
    }
  }
}

void ConnectionManager::handleConnStatus(const NodeNum destination, std::promise<bool>& connected) const {
  connected.set_value(connections_.count(destination) ? true : false);
}

void ConnectionManager::remoteCloseConnection(NodeNum id) {
  asio::post(strand_, [this, id]() {
    // This check is because of a race condition.
    // It's possible that the ConnMgr can call conn->remoteDispose() as a result of it destroying
    // the connection, but that the socket gets closed resulting in a simultaneous call here to
    // remoteCloseConnection from the AsyncTlsConnection. In this case, the connection may already
    // have been erased.
    if (!connections_.count(id)) return;

    LOG_INFO(logger_, "Closing connection from: " << config_.selfId << ", to: " << id);
    auto conn = std::move(connections_.at(id));
    connections_.erase(id);
    status_->num_connections = connections_.size();
    conn->close();
  });
}

void ConnectionManager::closeConnection(std::shared_ptr<AsyncTlsConnection> conn) {
  conn->remoteDispose();
  conn->close();
}

void ConnectionManager::syncCloseConnection(std::shared_ptr<AsyncTlsConnection>& conn) { conn->close(); }

void ConnectionManager::onConnectionAuthenticated(std::shared_ptr<AsyncTlsConnection> conn) {
  // Move the connection into the accepted connections map. If there is an existing connection
  // discard it. In this case it was likely that connecting end of the connection thinks there is
  // something wrong. This is a vector for a denial of service attack on the accepting side. We can
  // track the number of connections from the node and mark it malicious if necessary.
  concord::diagnostics::TimeRecorder scoped_timer(*histograms_.on_connection_authenticated);
  auto it = connections_.find(conn->getPeerId().value());
  if (it != connections_.end()) {
    LOG_INFO(logger_,
             "New connection accepted from same peer. Closing existing connection to " << conn->getPeerId().value());
    closeConnection(std::move(it->second));
  }
  connections_.insert_or_assign(conn->getPeerId().value(), conn);
  status_->num_connections = connections_.size();
  conn->startReading();
}

void ConnectionManager::onServerHandshakeComplete(const asio::error_code& ec, size_t accepted_connection_id) {
  auto conn = std::move(accepted_waiting_for_handshake_.at(accepted_connection_id));
  accepted_waiting_for_handshake_.erase(accepted_connection_id);
  status_->num_accepted_waiting_for_handshake = accepted_waiting_for_handshake_.size();
  if (ec) {
    auto peer_str = conn->getPeerId().has_value() ? std::to_string(conn->getPeerId().value()) : "Unknown";
    LOG_WARN(logger_, "Server handshake failed for peer " << peer_str << ": " << ec.message());
    return closeConnection(std::move(conn));
  }
  LOG_INFO(logger_, "Server handshake succeeded for peer " << conn->getPeerId().value());
  onConnectionAuthenticated(std::move(conn));
}

void ConnectionManager::onClientHandshakeComplete(const asio::error_code& ec, NodeNum destination) {
  auto conn = std::move(connected_waiting_for_handshake_.at(destination));
  connected_waiting_for_handshake_.erase(destination);
  status_->num_connected_waiting_for_handshake = connected_waiting_for_handshake_.size();
  if (ec) {
    LOG_WARN(logger_, "Client handshake failed for peer " << conn->getPeerId().value() << ": " << ec.message());
    return closeConnection(std::move(conn));
  }
  status_->total_connect_attempts_completed++;
  LOG_INFO(logger_, "Client handshake succeeded for peer " << conn->getPeerId().value());
  onConnectionAuthenticated(std::move(conn));
}

void ConnectionManager::startServerSSLHandshake(asio::ip::tcp::socket&& socket) {
  auto connection_id = total_accepted_connections_;
  auto conn =
      AsyncTlsConnection::create(io_context_, std::move(socket), receiver_, *this, config_, *status_, histograms_);
  accepted_waiting_for_handshake_.insert({connection_id, conn});
  status_->num_accepted_waiting_for_handshake = accepted_waiting_for_handshake_.size();
  conn->getSocket().async_handshake(asio::ssl::stream_base::server,
                                    asio::bind_executor(strand_, [this, connection_id](const asio::error_code& ec) {
                                      onServerHandshakeComplete(ec, connection_id);
                                    }));
}

void ConnectionManager::startClientSSLHandshake(asio::ip::tcp::socket&& socket, NodeNum destination) {
  auto conn = AsyncTlsConnection::create(
      io_context_, std::move(socket), receiver_, *this, destination, config_, *status_, histograms_);
  connected_waiting_for_handshake_.insert({destination, conn});
  status_->num_connected_waiting_for_handshake = connected_waiting_for_handshake_.size();
  conn->getSocket().async_handshake(asio::ssl::stream_base::client,
                                    asio::bind_executor(strand_, [this, destination](const asio::error_code& ec) {
                                      onClientHandshakeComplete(ec, destination);
                                    }));
}

void ConnectionManager::accept() {
  acceptor_.async_accept(asio::bind_executor(strand_, [this](asio::error_code ec, asio::ip::tcp::socket sock) {
    if (stopped_) return;
    if (!StateControl::instance().tryLockComm()) {
      LOG_WARN(logger_, "incoming comm is blocked");
      return;
    }
    if (ec) {
      LOG_WARN(logger_, "async_accept failed: " << ec.message());
      // When io_service is stopped, the handlers are destroyed and when the
      // io_service dtor runs they will be invoked with operation_aborted error.
      // In this case we dont want to accept again.
      if (ec == asio::error::operation_aborted) {
        StateControl::instance().unlockComm();
        return;
      }
    } else {
      total_accepted_connections_++;
      status_->total_accepted_connections = total_accepted_connections_;
      setSocketOptions(sock);
      LOG_INFO(logger_, "Accepted connection " << total_accepted_connections_);
      startServerSSLHandshake(std::move(sock));
      StateControl::instance().unlockComm();
    }
    accept();
  }));
}

void ConnectionManager::resolve(NodeNum i) {
  resolving_.insert(i);
  status_->num_resolving = resolving_.size();
  auto node = config_.nodes.at(i);
  resolver_.async_resolve(
      asio::ip::tcp::v4(),
      node.host,
      std::to_string(node.port),
      asio::bind_executor(strand_, [this, node, i](const auto& error_code, auto results) {
        if (error_code) {
          LOG_WARN(
              logger_,
              "Failed to resolve node " << i << ": " << node.host << ":" << node.port << " : " << error_code.message());
          resolving_.erase(i);
          status_->num_resolving = resolving_.size();
          return;
        }
        asio::ip::tcp::endpoint endpoint = *results;
        LOG_INFO(logger_, "Resolved node " << i << ": " << node.host << ":" << node.port << " to " << endpoint);
        resolving_.erase(i);
        status_->num_resolving = resolving_.size();
        connect(i, endpoint);
      }));
}

void ConnectionManager::connect(NodeNum i, const asio::ip::tcp::endpoint& endpoint) {
  auto [it, inserted] = connecting_.emplace(i, asio::ip::tcp::socket(io_context_));
  ConcordAssert(inserted);
  status_->num_connecting = connecting_.size();
  it->second.async_connect(
      endpoint, asio::bind_executor(strand_, [this, i, endpoint](const auto& error_code) {
        if (error_code) {
          LOG_WARN(logger_, "Failed to connect to node " << i << ": " << endpoint << " : " << error_code.message());
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
      }));
}

void ConnectionManager::connect() {
  auto end = std::min<size_t>(config_.selfId, config_.maxServerId + 1);
  for (auto i = 0u; i < end; i++) {
    if (connections_.count(i) == 0 && connecting_.count(i) == 0 && resolving_.count(i) == 0 &&
        connected_waiting_for_handshake_.count(i) == 0) {
      resolve(i);
    }
  }
}

asio::ip::tcp::endpoint ConnectionManager::syncResolve() {
  auto results = resolver_.resolve(asio::ip::tcp::v4(), config_.listenHost, std::to_string(config_.listenPort));
  asio::ip::tcp::endpoint endpoint = *results;
  LOG_INFO(logger_, "Resolved " << config_.listenHost << ":" << config_.listenPort << " to " << endpoint);
  return endpoint;
}

int ConnectionManager::getMaxMessageSize() const { return static_cast<int>(config_.bufferLength); }

ConnectionStatus ConnectionManager::getCurrentConnectionStatus(const NodeNum id) const {
  std::promise<bool> connected;
  auto future = connected.get_future();
  asio::post(strand_, [this, id, &connected]() { handleConnStatus(id, connected); });
  if (future.get()) {
    return ConnectionStatus::Connected;
  }
  return ConnectionStatus::Disconnected;
}
}  // namespace bft::communication::tls
