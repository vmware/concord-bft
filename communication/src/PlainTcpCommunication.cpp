// Concord
//
// Copyright (c) 2018-2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, anoted in the LICENSE file.

#include <unordered_map>
#include <string>
#include <functional>
#include <iostream>
#include <sstream>
#include <thread>
#include <cstring>
#include <chrono>
#include <mutex>
#include <cassert>

#include <execinfo.h>
#include <unistd.h>
#include <sys/time.h>

#include "communication/CommDefs.hpp"
#include "Logger.hpp"
#include "boost/bind.hpp"
#include <boost/asio.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/move/unique_ptr.hpp>
#include <boost/make_shared.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/smart_ptr/scoped_ptr.hpp>
#include <boost/make_unique.hpp>
#include <boost/asio/deadline_timer.hpp>
#include <boost/date_time/posix_time/posix_time_duration.hpp>

#include "assertUtils.hpp"

using namespace std;
using namespace boost::asio;
using namespace boost::asio::ip;
using namespace boost::posix_time;

using boost::asio::io_service;
using boost::system::error_code;
using boost::asio::ip::address;

namespace bft::communication {

class AsyncTcpConnection;

typedef boost::system::error_code B_ERROR_CODE;
typedef boost::shared_ptr<AsyncTcpConnection> ASYNC_CONN_PTR;
typedef tcp::socket B_TCP_SOCKET;

// first 4 bytes - message length, next 2 bytes - message type
static constexpr uint8_t LENGTH_FIELD_SIZE = 4;
static constexpr uint8_t MSGTYPE_FIELD_SIZE = 2;

enum MessageType : uint16_t { Reserved = 0, Hello, Regular };

enum ConnType : uint8_t { Incoming, Outgoing };

/** this class will handle single connection using boost::make_shared idiom
 * will receive the IReceiver as a parameter and call it when new message
 * is available
 * TODO(IG): add multithreading
 */
class AsyncTcpConnection : public boost::enable_shared_from_this<AsyncTcpConnection> {
 private:
  bool _isReplica = false;
  bool _destIsReplica = false;
  io_service *_service = nullptr;
  uint32_t _bufferLength;
  char *_inBuffer = nullptr;
  char *_outBuffer = nullptr;
  IReceiver *_receiver = nullptr;
  function<void(NodeNum)> _fOnError = nullptr;
  function<void(NodeNum, ASYNC_CONN_PTR)> _fOnHellOMessage = nullptr;
  NodeNum _destId;
  NodeNum _selfId;
  string _host = "";
  uint16_t _port = 0;
  deadline_timer _connectTimer;
  ConnType _connType;
  bool _closed;
  logging::Logger _logger;
  uint16_t _minTimeout = 256;
  uint16_t _maxTimeout = 8192;
  uint16_t _currentTimeout = _minTimeout;
  bool _wasError = false;
  bool _connecting = false;
  UPDATE_CONNECTIVITY_FN _statusCallback = nullptr;
  NodeMap _nodes;
  recursive_mutex _connectionsGuard;

 public:
  bool isConnected() const { return connected; }

 public:
  B_TCP_SOCKET socket;
  bool connected;

 private:
  AsyncTcpConnection(io_service *service,
                     function<void(NodeNum)> onError,
                     function<void(NodeNum, ASYNC_CONN_PTR)> onHelloMsg,
                     uint32_t bufferLength,
                     NodeNum destId,
                     NodeNum selfId,
                     ConnType type,
                     logging::Logger logger,
                     UPDATE_CONNECTIVITY_FN statusCallback,
                     NodeMap nodes)
      : _service(service),
        _bufferLength(bufferLength),
        _fOnError(onError),
        _fOnHellOMessage(onHelloMsg),
        _destId(destId),
        _selfId(selfId),
        _connectTimer(*service),
        _connType(type),
        _closed(false),
        _logger(logger),
        _statusCallback{statusCallback},
        _nodes{std::move(nodes)},
        socket(*service),
        connected(false) {
    LOG_TRACE(_logger, "enter, node " << _selfId << ", dest: " << _destId);

    _isReplica = check_replica(_selfId);
    _inBuffer = new char[bufferLength];
    _outBuffer = new char[bufferLength];

    _connectTimer.expires_at(boost::posix_time::pos_infin);

    LOG_TRACE(_logger, "exit, node " << _selfId << ", dest: " << _destId);
  }

  bool check_replica(NodeNum node) {
    auto it = _nodes.find(node);
    if (it == _nodes.end()) {
      return false;
    }

    return it->second.isReplica;
  }

  void parse_message_header(const char *buffer, uint32_t &msgLength) {
    msgLength = *(static_cast<const uint32_t *>(static_cast<const void *>(buffer)));
  }

  void close_socket() {
    LOG_TRACE(
        _logger,
        "enter, node " << _selfId << ", dest: " << _destId << ", connected: " << connected << ", closed: " << _closed);

    try {
      boost::system::error_code ignored_ec;
      socket.shutdown(boost::asio::ip::tcp::socket::shutdown_both, ignored_ec);
      socket.close();
    } catch (std::exception &e) {
      LOG_ERROR(_logger,
                "exception, node " << _selfId << ", dest: " << _destId << ", connected: " << connected
                                   << ", ex: " << e.what());
    }

    LOG_TRACE(
        _logger,
        "exit, node " << _selfId << ", dest: " << _destId << ", connected: " << connected << ", closed: " << _closed);
  }

  void close() {
    _connecting = true;
    LOG_TRACE(
        _logger,
        "enter, node " << _selfId << ", dest: " << _destId << ", connected: " << connected << ", closed: " << _closed);

    lock_guard<recursive_mutex> lock(_connectionsGuard);

    connected = false;
    _closed = true;
    _connectTimer.cancel();

    try {
      B_ERROR_CODE ec;
      socket.shutdown(boost::asio::ip::tcp::socket::shutdown_both, ec);
      socket.close();
    } catch (std::exception &e) {
      LOG_ERROR(_logger,
                "exception, node " << _selfId << ", dest: " << _destId << ", connected: " << connected
                                   << ", ex: " << e.what());
    }

    LOG_TRACE(
        _logger,
        "exit, node " << _selfId << ", dest: " << _destId << ", connected: " << connected << ", closed: " << _closed);

    _fOnError(_destId);
  }

  bool was_error(const B_ERROR_CODE &ec, string where) {
    if (ec)
      LOG_ERROR(_logger,
                "where: " << where << ", node " << _selfId << ", dest: " << _destId << ", connected: " << connected
                          << ", ex: " << ec.message());

    return (ec != 0);
  }

  void reconnect() {
    _connecting = true;

    LOG_TRACE(_logger,
              "enter, node " << _selfId << ", dest: " << _destId << ", connected: " << connected
                             << "is_open: " << socket.is_open());

    lock_guard<recursive_mutex> lock(_connectionsGuard);

    connected = false;
    close_socket();

    socket = B_TCP_SOCKET(*_service);

    setTimeOut();
    connect(_host, _port, _destIsReplica);

    LOG_TRACE(_logger,
              "exit, node " << _selfId << ", dest: " << _destId << ", connected: " << connected
                            << "is_open: " << socket.is_open());
  }

  void handle_error(B_ERROR_CODE ec) {
    if (boost::asio::error::operation_aborted == ec) {
      return;
    }

    if (ConnType::Incoming == _connType) {
      close();
    } else {
      reconnect();
      if (_statusCallback) {
        bool isReplica = check_replica(_selfId);
        if (isReplica) {
          PeerConnectivityStatus pcs{};
          pcs.peerId = _selfId;
          pcs.statusType = StatusType::Broken;

          // pcs.statusTime = we dont set it since it is set by the aggregator
          // in the upcoming version timestamps should be reviewed
          _statusCallback(pcs);
        }
      }
    }
  }

  void read_header_async_completed(const B_ERROR_CODE &ec, const uint32_t bytesRead) {
    LOG_TRACE(_logger,
              "enter, node " << _selfId << ", dest: " << _destId << ", connected: " << connected
                             << "is_open: " << socket.is_open());

    lock_guard<recursive_mutex> lock(_connectionsGuard);

    if (_wasError || _connecting) {
      LOG_TRACE(_logger, "was error, node " << _selfId << ", dest: " << _destId);
      return;
    }

    auto err = was_error(ec, __func__);
    if (err) {
      handle_error(ec);
      return;
    }

    uint32_t msgLength;
    parse_message_header(_inBuffer, msgLength);
    if (msgLength == 0) {
      LOG_ERROR(_logger, "on_read_async_header_completed, msgLen=0");
      return;
    }

    read_msg_async(LENGTH_FIELD_SIZE, msgLength);

    LOG_TRACE(_logger,
              "exit, node " << _selfId << ", dest: " << _destId << ", connected: " << connected
                            << "is_open: " << socket.is_open());
  }

  void read_header_async() {
    LOG_TRACE(_logger,
              "enter, node " << _selfId << ", dest: " << _destId << ", connected: " << connected
                             << "is_open: " << socket.is_open());

    memset(_inBuffer, 0, _bufferLength);
    async_read(socket,
               buffer(_inBuffer, LENGTH_FIELD_SIZE),
               boost::bind(&AsyncTcpConnection::read_header_async_completed,
                           shared_from_this(),
                           boost::asio::placeholders::error,
                           boost::asio::placeholders::bytes_transferred));

    LOG_TRACE(_logger,
              "exit, node " << _selfId << ", dest: " << _destId << ", connected: " << connected
                            << "is_open: " << socket.is_open());
  }

  bool is_service_message() {
    uint16_t msgType = *(static_cast<uint16_t *>(static_cast<void *>(_inBuffer + LENGTH_FIELD_SIZE)));
    switch (msgType) {
      case MessageType::Hello:
        _destId = *(static_cast<NodeNum *>(static_cast<void *>(_inBuffer + LENGTH_FIELD_SIZE + MSGTYPE_FIELD_SIZE)));

        LOG_DEBUG(_logger, "node: " << _selfId << " got hello from:" << _destId);

        _fOnHellOMessage(_destId, shared_from_this());
        _destIsReplica = check_replica(_destId);
        LOG_DEBUG(_logger, "node: " << _selfId << " dest is replica: " << _destIsReplica);
        return true;
      default:
        return false;
    }
  }

  void read_msg_async_completed(const boost::system::error_code &ec, size_t bytesRead) {
    LOG_TRACE(_logger, "enter, node " << _selfId << ", dest: " << _destId);

    lock_guard<recursive_mutex> lock(_connectionsGuard);

    if (_wasError || _connecting) {
      LOG_TRACE(_logger, "was error, node " << _selfId << ", dest: " << _destId);
      return;
    }

    auto err = was_error(ec, __func__);
    if (err) {
      _wasError = true;
      return;
    }

    if (!is_service_message()) {
      LOG_DEBUG(_logger, "data msg received, msgLen: " << bytesRead);
      _receiver->onNewMessage(
          _destId, _inBuffer + LENGTH_FIELD_SIZE + MSGTYPE_FIELD_SIZE, bytesRead - MSGTYPE_FIELD_SIZE);
    }

    read_header_async();

    if (_statusCallback && _destIsReplica) {
      PeerConnectivityStatus pcs{};
      pcs.peerId = _destId;
      pcs.peerHost = _host;
      pcs.peerPort = _port;
      pcs.statusType = StatusType::MessageReceived;

      // pcs.statusTime = we dont set it since it is set by the aggregator
      // in the upcoming version timestamps should be reviewed
      _statusCallback(pcs);
    }

    LOG_TRACE(_logger, "exit, node " << _selfId << ", dest: " << _destId);
  }

  void read_msg_async(uint32_t offset, uint32_t msgLength) {
    LOG_TRACE(_logger, "enter, node " << _selfId << ", dest: " << _destId);

    // async operation will finish when either expectedBytes are read
    // or error occured
    async_read(socket,
               boost::asio::buffer(_inBuffer + offset, msgLength),
               boost::bind(&AsyncTcpConnection::read_msg_async_completed,
                           shared_from_this(),
                           boost::asio::placeholders::error,
                           boost::asio::placeholders::bytes_transferred));

    LOG_TRACE(_logger, "exit, node " << _selfId << ", dest: " << _destId);
  }

  void write_async_completed(const B_ERROR_CODE &err, size_t bytesTransferred) {
    LOG_TRACE(_logger, "enter, node " << _selfId << ", dest: " << _destId);

    if (_wasError) {
      LOG_TRACE(_logger, "was error, node " << _selfId << ", dest: " << _destId);
      return;
    }

    auto res = was_error(err, __func__);

    if (res) {
      _wasError = true;
      return;
    }

    LOG_TRACE(_logger, "exit, node " << _selfId << ", dest: " << _destId);
  }

  uint16_t prepare_output_buffer(uint16_t msgType, uint32_t dataLength) {
    memset(_outBuffer, 0, _bufferLength);
    uint32_t size = sizeof(msgType) + dataLength;
    memcpy(_outBuffer, &size, LENGTH_FIELD_SIZE);
    memcpy(_outBuffer + LENGTH_FIELD_SIZE, &msgType, MSGTYPE_FIELD_SIZE);
    return LENGTH_FIELD_SIZE + MSGTYPE_FIELD_SIZE;
  }

  void send_hello() {
    auto offset = prepare_output_buffer(MessageType::Hello, sizeof(_selfId));
    memcpy(_outBuffer + offset, &_selfId, sizeof(_selfId));

    LOG_DEBUG(_logger,
              "sending hello from:" << _selfId << " to: " << _destId << ", size: " << (offset + sizeof(_selfId)));

    AsyncTcpConnection::write_async((const char *)_outBuffer, offset + sizeof(_selfId));
  }

  void setTimeOut() { _currentTimeout = _currentTimeout == _maxTimeout ? _minTimeout : _currentTimeout * 2; }

  void connect_timer_tick(const B_ERROR_CODE &ec) {
    LOG_TRACE(_logger, "enter, node " << _selfId << ", dest: " << _destId << ", ec: " << ec.message());

    if (_closed) {
      LOG_DEBUG(_logger, "closed, node " << _selfId << ", dest: " << _destId << ", ec: " << ec.message());
    } else {
      if (connected) {
        LOG_DEBUG(_logger, "already connected, node " << _selfId << ", dest: " << _destId << ", ec: " << ec);
        _connectTimer.expires_at(boost::posix_time::pos_infin);
      } else if (_connectTimer.expires_at() <= deadline_timer::traits_type::now()) {
        LOG_DEBUG(_logger, "reconnecting, node " << _selfId << ", dest: " << _destId << ", ec: " << ec);
        reconnect();
      } else {
        LOG_DEBUG(_logger, "else, node " << _selfId << ", dest: " << _destId << ", ec: " << ec.message());
      }

      _connectTimer.async_wait(
          boost::bind(&AsyncTcpConnection::connect_timer_tick, shared_from_this(), boost::asio::placeholders::error));
    }

    LOG_TRACE(_logger, "exit, node " << _selfId << ", dest: " << _destId << ", ec: " << ec.message());
  }

  void connect_completed(const B_ERROR_CODE &err) {
    LOG_TRACE(_logger, "enter, node " << _selfId << ", dest: " << _destId);

    lock_guard<recursive_mutex> lock(_connectionsGuard);
    auto res = was_error(err, __func__);

    if (!socket.is_open()) {
      // async_connect opens socket on start so
      // nothing to do here since timeout occured and closed the socket
      if (connected) {
        LOG_DEBUG(_logger, "node " << _selfId << " is DISCONNECTED from node " << _destId);
      }
      connected = false;
    } else if (res) {
      connected = false;
      // timeout didnt happen yet but the connection failed
      // nothig to do here, left for clarity
    } else {
      LOG_DEBUG(_logger, "connected, node " << _selfId << ", dest: " << _destId << ", res: " << res);
      connected = true;
      _wasError = false;
      _connecting = false;
      _connectTimer.expires_at(boost::posix_time::pos_infin);
      _currentTimeout = _minTimeout;
      send_hello();
      read_header_async();
    }

    LOG_TRACE(_logger, "exit, node " << _selfId << ", dest: " << _destId);
  }

  void write_async(const char *data, uint32_t length) {
    if (!connected) return;

    B_ERROR_CODE ec;
    write(socket, buffer(data, length), ec);
    auto err = was_error(ec, __func__);
    if (err) {
      handle_error(ec);
    }
  }

  void init() {
    _connectTimer.async_wait(
        boost::bind(&AsyncTcpConnection::connect_timer_tick, shared_from_this(), boost::asio::placeholders::error));
  }

 public:
  void connect(string host, uint16_t port, bool destIsReplica) {
    _host = host;
    _port = port;
    _destIsReplica = destIsReplica;

    LOG_TRACE(_logger, "enter, from: " << _selfId << " ,to: " << _destId << ", host: " << host << ", port: " << port);

    tcp::resolver::query query(tcp::v4(), _host, std::to_string(_port));
    tcp::resolver resolver(*_service);
    boost::system::error_code ec;
    tcp::resolver::iterator results = resolver.resolve(query, ec);
    if (!ec && results != tcp::resolver::iterator()) {
      tcp::endpoint ep = *results;

      LOG_DEBUG(_logger,
                "connecting from: " << _selfId << " ,to: " << _destId << ", timeout: " << _currentTimeout
                                    << ", dest is replica: " << _destIsReplica);

      _connectTimer.expires_from_now(boost::posix_time::millisec(_currentTimeout));

      socket.async_connect(
          ep,
          boost::bind(&AsyncTcpConnection::connect_completed, shared_from_this(), boost::asio::placeholders::error));
    } else {
      LOG_INFO(_logger, "Unable to resolve " << host << ":" << port);
      if (!ec) {
        ec = boost::system::errc::make_error_code(boost::system::errc::connection_aborted);
      }
      // Use the async completion handler directly to kick off a retry.
      connect_completed(ec);
    }
    LOG_TRACE(_logger, "exit, from: " << _selfId << " ,to: " << _destId << ", host: " << host << ", port: " << port);
  }

  void start() { read_header_async(); }

  void send(const char *data, uint32_t length) {
    LOG_TRACE(_logger, "enter, node " << _selfId << ", dest: " << _destId);

    lock_guard<recursive_mutex> lock(_connectionsGuard);
    auto offset = prepare_output_buffer(MessageType::Regular, length);
    memcpy(_outBuffer + offset, data, length);
    write_async(_outBuffer, offset + length);

    if (_statusCallback && _isReplica) {
      PeerConnectivityStatus pcs{};
      pcs.peerId = _selfId;
      pcs.statusType = StatusType::MessageSent;

      // pcs.statusTime = we dont set it since it is set by the aggregator
      // in the upcoming version timestamps should be reviewed
      _statusCallback(pcs);
    }

    LOG_DEBUG(_logger,
              "send exit, from: "
                  << ", to: " << _destId << ", offset: " << offset << ", length: " << length);
    LOG_TRACE(_logger, "exit, node " << _selfId << ", dest: " << _destId);
  }

  static ASYNC_CONN_PTR create(io_service *service,
                               function<void(NodeNum)> onError,
                               function<void(NodeNum, ASYNC_CONN_PTR)> onHello,
                               uint32_t bufferLength,
                               NodeNum destId,
                               NodeNum selfId,
                               ConnType type,
                               logging::Logger logger,
                               UPDATE_CONNECTIVITY_FN statusCallback,
                               NodeMap nodes) {
    auto res = ASYNC_CONN_PTR(new AsyncTcpConnection(
        service, onError, onHello, bufferLength, destId, selfId, type, logger, statusCallback, nodes));
    res->init();
    return res;
  }

  void setReceiver(IReceiver *rec) { _receiver = rec; }

  virtual ~AsyncTcpConnection() {
    LOG_TRACE(
        _logger,
        "enter, node " << _selfId << ", dest: " << _destId << ", connected: " << connected << ", closed: " << _closed);

    delete[] _inBuffer;
    delete[] _outBuffer;

    LOG_TRACE(
        _logger,
        "exit, node " << _selfId << ", dest: " << _destId << ", connected: " << connected << ", closed: " << _closed);
  }
};

///////////////////////////////////////////////////////////////////////////////

class PlainTCPCommunication::PlainTcpImpl {
 private:
  unordered_map<NodeNum, ASYNC_CONN_PTR> _connections;
  logging::Logger _logger = logging::getLogger("concord-bft.tcp");

  unique_ptr<tcp::acceptor> _pAcceptor;
  std::thread *_pIoThread = nullptr;

  NodeNum _selfId;
  IReceiver *_pReceiver;

  io_service _service;
  uint16_t _listenPort;
  string _listenHost;
  uint32_t _bufferLength;
  uint32_t _maxServerId;
  UPDATE_CONNECTIVITY_FN _statusCallback = nullptr;
  recursive_mutex _connectionsGuard;

  void on_async_connection_error(NodeNum peerId) {
    LOG_ERROR(_logger, "to: " << peerId);
    lock_guard<recursive_mutex> lock(_connectionsGuard);
    _connections.erase(peerId);
  }

  void on_hello_message(NodeNum id, ASYNC_CONN_PTR conn) {
    LOG_DEBUG(_logger, "node: " << _selfId << ", from: " << id);

    //* potential fix for segment fault *//
    lock_guard<recursive_mutex> lock(_connectionsGuard);
    _connections.insert(make_pair(id, conn));
    conn->setReceiver(_pReceiver);
  }

  void on_accept(ASYNC_CONN_PTR conn, const NodeMap &nodes, const B_ERROR_CODE &ec) {
    LOG_TRACE(_logger, "enter, node: " << _selfId << ", ec: " << ec.message());

    if (!ec) {
      conn->connected = true;
      conn->start();
    }

    start_accept(nodes);
    LOG_TRACE(_logger, "exit, node: " << _selfId << "ec: " << ec.message());
  }

  // here need to check how "this" passed to handlers behaves
  // if the object is deleted.
  void start_accept(const NodeMap &nodes) {
    LOG_TRACE(_logger, "enter, node: " << _selfId);
    auto conn = AsyncTcpConnection::create(
        &_service,
        std::bind(&PlainTcpImpl::on_async_connection_error, this, std::placeholders::_1),
        std::bind(&PlainTcpImpl::on_hello_message, this, std::placeholders::_1, std::placeholders::_2),
        _bufferLength,
        0,
        _selfId,
        ConnType::Incoming,
        _logger,
        _statusCallback,
        nodes);
    _pAcceptor->async_accept(
        conn->socket, boost::bind(&PlainTcpImpl::on_accept, this, conn, nodes, boost::asio::placeholders::error));
    LOG_TRACE(_logger, "exit, node: " << _selfId);
  }

  PlainTcpImpl(const PlainTcpImpl &) = delete;             // non construction-copyable
  PlainTcpImpl(const PlainTcpImpl &&) = delete;            // non movable
  PlainTcpImpl &operator=(const PlainTcpImpl &) = delete;  // non copyable
  PlainTcpImpl() = delete;

  PlainTcpImpl(NodeNum selfNodeId,
               NodeMap nodes,
               uint32_t bufferLength,
               uint16_t listenPort,
               uint32_t maxServerId,
               string listenHost,
               UPDATE_CONNECTIVITY_FN statusCallback)
      : _selfId{selfNodeId},
        _listenPort{listenPort},
        _listenHost{listenHost},
        _bufferLength{bufferLength},
        _maxServerId{maxServerId},
        _statusCallback{statusCallback} {
    // all replicas are in listen mode
    if (_selfId <= _maxServerId) {
      tcp::resolver::query query(tcp::v4(), _listenHost, std::to_string(_listenPort));
      tcp::resolver resolver(_service);
      // This throws an exception if it can't resolve, but since this is the
      // *listen* port, we should expect to be able to resolve our own name, or
      // shutdown otherwise.
      tcp::resolver::iterator results = resolver.resolve(query);
      ConcordAssert(results != tcp::resolver::iterator());
      // Use the first result
      tcp::endpoint ep = *results;
      LOG_INFO(_logger, "Resolved " << _listenHost << ":" << _listenPort << " to " << ep);
      _pAcceptor = boost::make_unique<tcp::acceptor>(_service, ep);
      start_accept(nodes);
    } else  // clients dont need to listen
      LOG_DEBUG(_logger, "skipping listen for node: " << _selfId);

    // this node should connect only to nodes with lower ID
    // and all nodes with higher ID will connect to this node
    // we dont want that clients will connect to another clients
    for (auto it = nodes.begin(); it != nodes.end(); it++) {
      if (it->first < _selfId && it->first <= maxServerId) {
        auto conn = AsyncTcpConnection::create(
            &_service,
            std::bind(&PlainTcpImpl::on_async_connection_error, this, std::placeholders::_1),
            std::bind(&PlainTcpImpl::on_hello_message, this, std::placeholders::_1, std::placeholders::_2),
            _bufferLength,
            it->first,
            _selfId,
            ConnType::Outgoing,
            _logger,
            _statusCallback,
            nodes);

        _connections.insert(make_pair(it->first, conn));
        string peerHost = it->second.host;
        uint16_t peerPort = it->second.port;
        conn->connect(peerHost, peerPort, it->second.isReplica);
        LOG_TRACE(_logger, "connect called for node " << to_string(it->first));
      }
      if (it->second.isReplica && _statusCallback) {
        PeerConnectivityStatus pcs{};
        pcs.peerId = it->first;
        pcs.peerHost = it->second.host;
        pcs.peerPort = it->second.port;
        pcs.statusType = StatusType::Started;

        // pcs.statusTime = we dont set it since it is set by the aggregator
        // in the upcoming version timestamps should be reviewed
        _statusCallback(pcs);
      }
    }
  }

 public:
  static PlainTcpImpl *create(NodeNum selfNodeId,
                              // tuple of host, listen port, bind port
                              NodeMap nodes,
                              uint32_t bufferLength,
                              uint16_t listenPort,
                              uint32_t tempHighestNodeForConnecting,
                              string listenHost,
                              UPDATE_CONNECTIVITY_FN statusCallback) {
    return new PlainTcpImpl(
        selfNodeId, nodes, bufferLength, listenPort, tempHighestNodeForConnecting, listenHost, statusCallback);
  }

  int Start() {
    if (_pIoThread) return 0;  // running

    _pIoThread = new std::thread(std::bind(
        static_cast<size_t (boost::asio::io_service::*)()>(&boost::asio::io_service::run), std::ref(_service)));
    return 0;
  }

  /**
   * Stops the object (including its internal threads).
   * On success, returns 0.
   */
  int Stop() {
    if (!_pIoThread) return 0;  // stopped

    _service.stop();
    _pIoThread->join();

    _connections.clear();

    return 0;
  }

  bool isRunning() const {
    if (!_pIoThread) return false;  // stopped
    return true;
  }

  ConnectionStatus getCurrentConnectionStatus(const NodeNum destNode) {
    if (!isRunning()) return ConnectionStatus::Disconnected;
    lock_guard<recursive_mutex> lock(_connectionsGuard);
    const auto &conn = _connections.find(destNode);
    if (conn != _connections.end()) {
      LOG_INFO(_logger, "Connection found from " << _selfId << " to " << destNode);
      if (conn->second->isConnected()) return ConnectionStatus::Connected;
    }
    LOG_INFO(_logger, "No connection exists from " << _selfId << " to " << destNode);
    return ConnectionStatus::Disconnected;
  }

  /**
   * Sends a message on the underlying communication layer to a given
   * destination node. Asynchronous (non-blocking) method.
   * Returns 0 on success.
   */
  int sendAsyncMessage(const NodeNum destNode, const char *const message, const size_t messageLength) {
    LOG_TRACE(_logger, "enter, from: " << _selfId << ", to: " << to_string(destNode));

    lock_guard<recursive_mutex> lock(_connectionsGuard);
    auto temp = _connections.find(destNode);
    if (temp != _connections.end()) {
      LOG_TRACE(_logger, "Connection found, from: " << _selfId << ", to: " << destNode);

      if (temp->second->connected) {
        temp->second->send(message, messageLength);
      } else {
        LOG_TRACE(_logger, "Connection found but disconnected, from: " << _selfId << ", to: " << destNode);
      }
    }

    LOG_TRACE(_logger, "exit, from: " << _selfId << ", to: " << destNode);

    return 0;
  }

  /// TODO(IG): return real max message size... what is should be for TCP?
  int getMaxMessageSize() { return -1; }

  void setReceiver(NodeNum receiverNum, IReceiver *receiver) {
    _pReceiver = receiver;
    for (auto conn : _connections) {
      conn.second->setReceiver(receiver);
    }
  }

  virtual ~PlainTcpImpl() {
    LOG_TRACE(_logger, "PlainTCPDtor");
    _pIoThread = nullptr;
  }
};

PlainTCPCommunication::~PlainTCPCommunication() {
  if (_ptrImpl) {
    delete _ptrImpl;
  }
}

PlainTCPCommunication::PlainTCPCommunication(const PlainTcpConfig &config) {
  _ptrImpl = PlainTcpImpl::create(config.selfId,
                                  config.nodes,
                                  config.bufferLength,
                                  config.listenPort,
                                  config.maxServerId,
                                  config.listenHost,
                                  config.statusCallback);
}

PlainTCPCommunication *PlainTCPCommunication::create(const PlainTcpConfig &config) {
  return new PlainTCPCommunication(config);
}

int PlainTCPCommunication::getMaxMessageSize() { return _ptrImpl->getMaxMessageSize(); }

int PlainTCPCommunication::start() { return _ptrImpl->Start(); }

int PlainTCPCommunication::stop() {
  if (!_ptrImpl) return 0;

  auto res = _ptrImpl->Stop();
  return res;
}

bool PlainTCPCommunication::isRunning() const { return _ptrImpl->isRunning(); }

ConnectionStatus PlainTCPCommunication::getCurrentConnectionStatus(const NodeNum node) {
  return _ptrImpl->getCurrentConnectionStatus(node);
}

int PlainTCPCommunication::send(NodeNum destNode, std::vector<uint8_t> &&msg) {
  return _ptrImpl->sendAsyncMessage(destNode, (char *)msg.data(), msg.size());
}

std::set<NodeNum> PlainTCPCommunication::send(std::set<NodeNum> dests, std::vector<uint8_t> &&msg) {
  std::set<NodeNum> failed_nodes;
  for (auto &d : dests) {
    if (_ptrImpl->sendAsyncMessage(d, (char *)msg.data(), msg.size()) != 0) {
      failed_nodes.insert(d);
    }
  }
  return failed_nodes;
}

void PlainTCPCommunication::setReceiver(NodeNum receiverNum, IReceiver *receiver) {
  _ptrImpl->setReceiver(receiverNum, receiver);
}

}  // namespace bft::communication
