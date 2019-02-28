// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the
// LICENSE file.

#include "CommImpl.hpp"
#include <string>
#include <functional>
#include <iostream>
#include <sstream>
#include <thread>
#include <chrono>
#include <mutex>
#include <regex>

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
#include <boost/filesystem.hpp>
#include <boost/asio/ssl.hpp>
#include "openssl/ssl.h"
#include <openssl/x509.h>
#include <openssl/x509v3.h>

class AsyncTlsConnection;

using namespace std;
using namespace bftEngine;
using namespace boost::asio;
using namespace boost::asio::ip;
using namespace boost::posix_time;

using boost::asio::io_service;
using boost::asio::ip::address;
using boost::system::error_code;

typedef boost::system::error_code B_ERROR_CODE;
typedef std::shared_ptr<AsyncTlsConnection> ASYNC_CONN_PTR;
typedef boost::asio::ssl::stream<tcp::socket> SSL_SOCKET;
typedef unique_ptr<SSL_SOCKET> B_TLS_SOCKET_PTR;

/// TODO(IG): to get rid of all global variables

// first 4 bytes - message length, next 2 bytes - message type
static constexpr uint8_t LENGTH_FIELD_SIZE = 4;
static constexpr uint8_t MSGTYPE_FIELD_SIZE = 2;

// levels aligned with boost log and log4j
enum LogLevel { all, trace, debug, info, warning, error, fatal, off };

enum MessageType : uint16_t { Reserved = 0, Hello, Regular };

enum ConnType : uint8_t { Incoming, Outgoing };

void getTime(std::stringstream &ss) {
#if defined(_WIN32)
  SYSTEMTIME sysTime;
  GetLocalTime(&sysTime);  // TODO(GG): GetSystemTime ???

  uint32_t hour = sysTime.wHour;
  uint32_t minute = sysTime.wMinute;
  uint32_t seconds = sysTime.wSecond;
  uint32_t milli = sysTime.wMilliseconds;
#else
  timeval t;
  gettimeofday(&t, NULL);

  uint32_t secondsInDay = t.tv_sec % (3600 * 24);

  uint32_t hour = secondsInDay / 3600;
  uint32_t minute = (secondsInDay % 3600) / 60;
  uint32_t seconds = secondsInDay % 60;
  uint32_t milli = t.tv_usec / 1000;
#endif
  ss << hour << ":" << minute << ":" << seconds << "." << milli;
}

LogLevel currentLogLevel = LogLevel::off;
mutex _logGuard;
recursive_mutex _connectionsGuard;

void log_write(std::ostringstream &ss) {
  lock_guard<mutex> lock(_logGuard);
  std::stringstream sstime;
  getTime(sstime);
  printf("%s %s", sstime.str().c_str(), ss.str().c_str());
}

#define LOG_DEBUG(txt)                                                      \
  {                                                                         \
    if (currentLogLevel <= LogLevel::debug) {                               \
      std::ostringstream oss;                                               \
      oss << " DEBUG: " << __func__ << ", line: " << __LINE__ << " " << txt \
          << endl;                                                          \
      log_write(oss);                                                       \
    }                                                                       \
  }

#define LOG_TRACE(txt)                                                      \
  {                                                                         \
    if (currentLogLevel <= LogLevel::trace) {                               \
      std::ostringstream oss;                                               \
      oss << " TRACE: " << __func__ << ", line: " << __LINE__ << " " << txt \
          << endl;                                                          \
      log_write(oss);                                                       \
    }                                                                       \
  }

#define LOG_ERROR(txt)                                                      \
  {                                                                         \
    if (currentLogLevel <= LogLevel::error) {                               \
      std::ostringstream oss;                                               \
      oss << " ERROR: " << __func__ << ", line: " << __LINE__ << " " << txt \
          << endl;                                                          \
      log_write(oss);                                                       \
    }                                                                       \
  }

#define LOG_INFO(txt)                                                      \
  {                                                                        \
    if (currentLogLevel <= LogLevel::info) {                               \
      std::ostringstream oss;                                              \
      oss << " INFO: " << __func__ << ", line: " << __LINE__ << " " << txt \
          << endl;                                                         \
      log_write(oss);                                                      \
    }                                                                      \
  }

/**
 * this class will handle single connection using boost::make_shared idiom
 * will receive the IReceiver as a parameter and call it when new message
 * available
 */
class AsyncTlsConnection : public enable_shared_from_this<AsyncTlsConnection> {
 private:
  unique_ptr<ssl::context> _pSslContext = nullptr;
  io_service *_service = nullptr;
  uint32_t _bufferLength;
  char *_inBuffer;
  char *_outBuffer;
  IReceiver *_receiver = nullptr;
  function<void(NodeNum)> _fOnError = nullptr;
  function<void(NodeNum, ASYNC_CONN_PTR)> _fOnHellOMessage = nullptr;
  NodeNum _destId;
  NodeNum _selfId;
  string _ip;
  uint16_t _port;
  deadline_timer _connectTimer;
  ConnType _connType;
  bool _closed;
  uint16_t _minTimeout = 256;
  uint16_t _maxTimeout = 8192;
  uint16_t _currentTimeout = _minTimeout;
  bool _wasError = false;
  bool _connecting = false;
  B_TLS_SOCKET_PTR _socket = nullptr;
  string _certificatesRootFolder;

 public:
  bool connected;

 private:
  AsyncTlsConnection(io_service *service,
                     function<void(NodeNum)> onError,
                     function<void(NodeNum, ASYNC_CONN_PTR)> onHelloMsg,
                     uint32_t bufferLength,
                     NodeNum destId,
                     NodeNum selfId,
                     string certificatesRootFolder,
                     ConnType type)
      : _service(service),
        _bufferLength(bufferLength),
        _fOnError(onError),
        _fOnHellOMessage(onHelloMsg),
        _destId(destId),
        _selfId(selfId),
        _connectTimer(*service),
        _connType(type),
        _closed(false),
        _certificatesRootFolder(certificatesRootFolder),
        connected(false) {
    LOG_TRACE("enter, node " << _selfId << ", dest: " << _destId);

    _inBuffer = new char[bufferLength];
    _outBuffer = new char[bufferLength];

    create_ssl_context();
    set_tls();

    _socket = B_TLS_SOCKET_PTR(new SSL_SOCKET(*service, *_pSslContext));

    _connectTimer.expires_at(boost::posix_time::pos_infin);
    LOG_TRACE("exit, node " << _selfId << ", dest: " << _destId);
  }

  void parse_message_header(const char *buffer, uint32_t &msgLength) {
    msgLength =
        *(static_cast<const uint32_t *>(static_cast<const void *>(buffer)));
  }

  void create_ssl_context() {
    _pSslContext = unique_ptr<ssl::context>(new ssl::context(
        _connType == ConnType::Incoming ? ssl::context::tlsv12_server
                                        : ssl::context::tlsv12_client));
  }

  void set_tls() {
    _pSslContext->set_verify_mode(ssl::verify_peer |
                                  ssl::verify_fail_if_no_peer_cert);
    if (ConnType::Incoming == _connType)
      set_tls_server();
    else
      set_tls_client();
  }

  void set_tls_server() {
    _pSslContext->set_options(boost::asio::ssl::context::default_workarounds |
                              boost::asio::ssl::context::no_sslv2 |
                              boost::asio::ssl::context::no_sslv3 |
                              boost::asio::ssl::context::no_tlsv1 |
                              boost::asio::ssl::context::no_tlsv1_1 |
                              boost::asio::ssl::context::single_dh_use);

    _pSslContext->set_verify_callback(boost::bind(
        &AsyncTlsConnection::verify_certificate_server, this, _1, _2));

    namespace fs = boost::filesystem;
    auto path = fs::path(_certificatesRootFolder) /
                fs::path(to_string(_selfId)) / fs::path("server");
    _pSslContext->use_certificate_chain_file(
        (path / fs::path("server.cert")).string());
    _pSslContext->use_private_key_file((path / fs::path("server.key")).string(),
                                       boost::asio::ssl::context::pem);
    _pSslContext->use_tmp_dh_file((path / fs::path("dh2048.pem")).string());
  }

  void set_tls_client() {
    namespace fs = boost::filesystem;
    auto path = fs::path(_certificatesRootFolder) /
                fs::path(to_string(_selfId)) / "client";
    auto serverPath = fs::path(_certificatesRootFolder) /
                      fs::path(to_string(_destId)) / "server";

    _pSslContext->set_verify_callback(boost::bind(
        &AsyncTlsConnection::verify_certificate_client, this, _1, _2));

    _pSslContext->use_certificate_chain_file((path / "client.cert").string());
    _pSslContext->use_private_key_file((path / "client.key").string(),
                                       boost::asio::ssl::context::pem);
    _pSslContext->use_tmp_dh_file((path / "dh2048.pem").string());

    _pSslContext->load_verify_file((serverPath / "server.cert").string());
  }

  bool verify_certificate_server(bool preverified,
                                 boost::asio::ssl::verify_context &ctx) {
    // here we dont need to check preverified value since it will be always
    // false - we dont provide client's verification file in the ctx
    // creation since we dont know which clients will connect to this node

    char subject[256];
    X509 *cert = X509_STORE_CTX_get_current_cert(ctx.native_handle());
    if (!cert) {
      LOG_ERROR("no certificate from client");
      return false;
    }

    X509_NAME_oneline(X509_get_subject_name(cert), subject, 256);
    LOG_DEBUG("Verifying client: " << subject << ", " << preverified);
    auto res = check_sertificate(cert, "client", string(subject));
    LOG_DEBUG("Manual verifying client: " << subject << ", " << res);
    return true;
  }

  bool verify_certificate_client(bool preverified,
                                 boost::asio::ssl::verify_context &ctx) {
    // here we need to return false since server verification should always
    // succeed. no need to manually check if default verification failed
    if (!preverified) {
      return false;
    }

    char subject[256];
    X509 *cert = X509_STORE_CTX_get_current_cert(ctx.native_handle());
    if (!cert) {
      LOG_ERROR("no certificate from server");
      return false;
    }
    X509_NAME_oneline(X509_get_subject_name(cert), subject, 256);
    LOG_DEBUG("Verifying server: " << subject << ", " << preverified);
    auto res = check_sertificate(cert, "server", string(subject), _destId);
    LOG_DEBUG("Manual verifying server: " << subject << ", " << res);
    return true;
  }

  /**
   * certificate pinning
   * check for specific certificate and do not rely on the chain authentication
   */
  bool check_sertificate(X509 *receivedCert,
                         string connectionType,
                         string subject,
                         NodeNum expectedPeerId = -1) {
    regex r("OU=\\d*", regex_constants::icase);
    smatch sm;
    regex_search(subject, sm, r);
    if (4 > sm.length()) {
      LOG_ERROR("OU not found or empty: " << subject);
      return false;
    }

    string remPeer = sm.str().substr(0, sm.str().length() - 3);
    if (0 == remPeer.length()) {
      LOG_ERROR("OU empty " << subject);
      return false;
    }

    int remotePeerId;
    try {
      remotePeerId = stoi(sm.str());
    } catch (const std::invalid_argument &ia) {
      LOG_ERROR("cannot convert OU, " << subject);
      return false;
    }

    // if server has been verified, check that we co
    if (-1 != expectedPeerId) {
      if (remotePeerId != expectedPeerId) {
        LOG_ERROR("peers doesnt match, expected: "
                  << expectedPeerId << ", received: " << remPeer);
        return false;
      }
    }

    namespace fs = boost::filesystem;
    auto path = fs::path(_certificatesRootFolder) / to_string(remotePeerId) /
                connectionType / string(connectionType + ".cert");

    FILE *fp = fopen(path.c_str(), "r");
    if (!fp) {
      LOG_ERROR("certificate file not found, path: " << path);
      return false;
    }

    X509 *localCert = PEM_read_X509(fp, NULL, NULL, NULL);
    if (!localCert) {
      LOG_ERROR("cannot parse certificate, path: " << path);
      fclose(fp);
      return false;
    }

    int res = X509_cmp(receivedCert, localCert);

    X509_free(localCert);
    fclose(fp);

    return res == 0;
  }

  void close_socket() {
    LOG_TRACE("enter, node " << _selfId << ", dest: " << _destId
                             << ", connected: " << connected
                             << ", closed: " << _closed);

    try {
      boost::system::error_code ignored_ec;
      get_socket().shutdown(boost::asio::ip::tcp::socket::shutdown_both,
                            ignored_ec);
      get_socket().close();
      if (_pSslContext) _pSslContext.release();
    } catch (std::exception &e) {
      LOG_ERROR("exception, node " << _selfId << ", dest: " << _destId
                                   << ", connected: " << connected
                                   << ", ex: " << e.what());
    }

    LOG_TRACE("exit, node " << _selfId << ", dest: " << _destId
                            << ", connected: " << connected
                            << ", closed: " << _closed);
  }

  void close() {
    _connecting = true;
    LOG_TRACE("enter, node " << _selfId << ", dest: " << _destId
                             << ", connected: " << connected
                             << ", closed: " << _closed);

    lock_guard<recursive_mutex> lock(_connectionsGuard);

    connected = false;
    _closed = true;
    _connectTimer.cancel();

    try {
      B_ERROR_CODE ec;
      get_socket().shutdown(boost::asio::ip::tcp::socket::shutdown_both, ec);
      get_socket().close();
      if (_pSslContext) _pSslContext.release();
    } catch (std::exception &e) {
      LOG_ERROR("exception, node " << _selfId << ", dest: " << _destId
                                   << ", connected: " << connected
                                   << ", ex: " << e.what());
    }

    LOG_TRACE("exit, node " << _selfId << ", dest: " << _destId
                            << ", connected: " << connected
                            << ", closed: " << _closed);

    _fOnError(_destId);
  }

  bool was_error(const B_ERROR_CODE &ec, string where) {
    if (ec)
      LOG_ERROR("where: " << where << ", node " << _selfId << ", dest: "
                          << _destId << ", connected: " << connected
                          << ", ex: " << ec.message());

    return (ec != 0);
  }

  void reconnect() {
    _connecting = true;

    LOG_TRACE("enter, node " << _selfId << ", dest: " << _destId
                             << ", connected: " << connected
                             << "is_open: " << get_socket().is_open());

    lock_guard<recursive_mutex> lock(_connectionsGuard);

    connected = false;
    close_socket();

    _pSslContext.reset(new ssl::context(_connType == ConnType::Incoming
                                            ? ssl::context::tls_server
                                            : ssl::context::tls_client));
    set_tls();
    _socket.reset(new SSL_SOCKET(*_service, *_pSslContext));

    setTimeOut();
    connect(_ip, _port);

    LOG_TRACE("exit, node " << _selfId << ", dest: " << _destId
                            << ", connected: " << connected
                            << "is_open: " << get_socket().is_open());
  }

  void handle_error(B_ERROR_CODE ec) {
    if (boost::asio::error::operation_aborted == ec) return;

    if (ConnType::Incoming == _connType)
      close();
    else
      reconnect();
  }

  void read_header_async_completed(const B_ERROR_CODE &ec,
                                   const uint32_t bytesRead) {
    LOG_TRACE("enter, node " << _selfId << ", dest: " << _destId
                             << ", connected: " << connected
                             << "is_open: " << get_socket().is_open());

    lock_guard<recursive_mutex> lock(_connectionsGuard);

    // (IG): patch, dont do it, need to fix multithreading
    if (_wasError || _connecting) {
      LOG_TRACE("was error, node " << _selfId << ", dest: " << _destId);
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
      LOG_ERROR("on_read_async_header_completed, msgLen=0");
      return;
    }

    read_msg_async(LENGTH_FIELD_SIZE, msgLength);

    LOG_TRACE("exit, node " << _selfId << ", dest: " << _destId
                            << ", connected: " << connected
                            << "is_open: " << get_socket().is_open());
  }

  void read_header_async() {
    LOG_TRACE("enter, node " << _selfId << ", dest: " << _destId
                             << ", connected: " << connected
                             << "is_open: " << get_socket().is_open());

    memset(_inBuffer, 0, _bufferLength);
    async_read(*_socket,
               buffer(_inBuffer, LENGTH_FIELD_SIZE),
               boost::bind(&AsyncTlsConnection::read_header_async_completed,
                           shared_from_this(),
                           boost::asio::placeholders::error,
                           boost::asio::placeholders::bytes_transferred));

    LOG_TRACE("exit, node " << _selfId << ", dest: " << _destId
                            << ", connected: " << connected
                            << "is_open: " << get_socket().is_open());
  }

  bool is_service_message() {
    uint16_t msgType = *(static_cast<uint16_t *>(
        static_cast<void *>(_inBuffer + LENGTH_FIELD_SIZE)));
    switch (msgType) {
      case MessageType::Hello:
        _destId = *(static_cast<NodeNum *>(static_cast<void *>(
            _inBuffer + LENGTH_FIELD_SIZE + MSGTYPE_FIELD_SIZE)));

        LOG_DEBUG("node: " << _selfId << " got hello from:" << _destId);

        _fOnHellOMessage(_destId, shared_from_this());
        return true;
        break;
      default:
        return false;
    }
  }

  void read_msg_async_completed(const boost::system::error_code &ec,
                                size_t bytesRead) {
    // (IG): patch, dont do it, need to fix multithreading
    LOG_TRACE("enter, node " << _selfId << ", dest: " << _destId);

    lock_guard<recursive_mutex> lock(_connectionsGuard);

    if (_wasError || _connecting) {
      LOG_TRACE("was error, node " << _selfId << ", dest: " << _destId);
      return;
    }

    auto err = was_error(ec, __func__);
    if (err) {
      // (IG): patch, dont do it, need to fix multithreading
      _wasError = true;
      /*
      if(ConnType::Incoming == _connType)
         close();
      else
         reconnect();
      */

      return;
    }

    if (!is_service_message()) {
      LOG_DEBUG("data msg received, msgLen: " << bytesRead);
      _receiver->onNewMessage(
          _destId,
          _inBuffer + LENGTH_FIELD_SIZE + MSGTYPE_FIELD_SIZE,
          bytesRead - MSGTYPE_FIELD_SIZE);
    }

    read_header_async();

    LOG_TRACE("exit, node " << _selfId << ", dest: " << _destId);
  }

  void read_msg_async(uint32_t offset, uint32_t msgLength) {
    LOG_TRACE("enter, node " << _selfId << ", dest: " << _destId);

    // async operation will finish when either expectedBytes are read
    // or error occured
    async_read(*_socket,
               boost::asio::buffer(_inBuffer + offset, msgLength),
               boost::bind(&AsyncTlsConnection::read_msg_async_completed,
                           shared_from_this(),
                           boost::asio::placeholders::error,
                           boost::asio::placeholders::bytes_transferred));

    LOG_TRACE("exit, node " << _selfId << ", dest: " << _destId);
  }

  void write_async_completed(const B_ERROR_CODE &err, size_t bytesTransferred) {
    LOG_TRACE("enter, node " << _selfId << ", dest: " << _destId);

    if (_wasError) {
      LOG_TRACE("was error, node " << _selfId << ", dest: " << _destId);
      return;
    }

    auto res = was_error(err, __func__);

    if (res) {
      _wasError = true;
      /*
      if(ConnType::Incoming == _connType)
         close();
      else
         reconnect();
      */
      return;
    }

    LOG_TRACE("exit, node " << _selfId << ", dest: " << _destId);
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

    LOG_DEBUG("sending hello from:" << _selfId << " to: " << _destId
                                    << ", size: "
                                    << (offset + sizeof(_selfId)));

    AsyncTlsConnection::write_async((const char *)_outBuffer,
                                    offset + sizeof(_selfId));
  }

  void setTimeOut() {
    _currentTimeout =
        _currentTimeout == _maxTimeout ? _minTimeout : _currentTimeout * 2;
  }

  void connect_timer_tick(const B_ERROR_CODE &ec) {
    LOG_TRACE("enter, node " << _selfId << ", dest: " << _destId
                             << ", ec: " << ec.message());

    if (_closed) {
      LOG_DEBUG("closed, node " << _selfId << ", dest: " << _destId
                                << ", ec: " << ec.message());
    } else {
      if (connected) {
        LOG_DEBUG("already connected, node " << _selfId << ", dest: " << _destId
                                             << ", ec: " << ec);
        _connectTimer.expires_at(boost::posix_time::pos_infin);
      } else if (_connectTimer.expires_at() <=
                 deadline_timer::traits_type::now()) {
        LOG_DEBUG("reconnecting, node " << _selfId << ", dest: " << _destId
                                        << ", ec: " << ec);
        reconnect();
      } else
        LOG_DEBUG("else, node " << _selfId << ", dest: " << _destId
                                << ", ec: " << ec.message());

      _connectTimer.async_wait(
          boost::bind(&AsyncTlsConnection::connect_timer_tick,
                      shared_from_this(),
                      boost::asio::placeholders::error));
    }

    LOG_TRACE("exit, node " << _selfId << ", dest: " << _destId
                            << ", ec: " << ec.message());
  }

  void connect_completed(const B_ERROR_CODE &err) {
    LOG_TRACE("enter, node " << _selfId << ", dest: " << _destId);

    lock_guard<recursive_mutex> lock(_connectionsGuard);
    auto res = was_error(err, __func__);

    if (!get_socket().is_open()) {
      // async_connect opens socket on start so
      // nothing to do here since timeout occured and closed the socket
      if (connected) {
        LOG_DEBUG("node " << _selfId << " is DISCONNECTED from node "
                          << _destId);
      }
      connected = false;
    } else if (res) {
      connected = false;
      // timeout didnt happen yet but the connection failed
      // nothig to do here, left for clarity
    } else {
      LOG_DEBUG("connected, node " << _selfId << ", dest: " << _destId
                                   << ", res: " << res);

      _socket->async_handshake(
          boost::asio::ssl::stream_base::client,
          boost::bind(&AsyncTlsConnection::handle_handshake,
                      this,
                      boost::asio::placeholders::error));
    }

    LOG_TRACE("exit, node " << _selfId << ", dest: " << _destId);
  }

  void handle_handshake(const B_ERROR_CODE &err) {
    if (!err) {
      connected = true;
      _wasError = false;
      _connecting = false;
      _connectTimer.expires_at(boost::posix_time::pos_infin);
      _currentTimeout = _minTimeout;
      send_hello();
      read_header_async();
    }
  }

  void handshake_completed(const B_ERROR_CODE &err) {
    auto wasError = was_error(err, "handshake_completed");
    if (!wasError) read_header_async();
  }

  void write_async(const char *data, uint32_t length) {
    // async_write(socket,
    //    buffer(data, length),
    //    boost::bind(&AsyncTcpConnection::write_async_completed,
    //       shared_from_this(),
    //       boost::asio::placeholders::error,
    //       boost::asio::placeholders::bytes_transferred));

    if (!connected) return;

    B_ERROR_CODE ec;
    write(*_socket, buffer(data, length), ec);
    auto err = was_error(ec, __func__);
    if (err) {
      handle_error(ec);
    }
  }

  void init() {
    _connectTimer.async_wait(
        boost::bind(&AsyncTlsConnection::connect_timer_tick,
                    shared_from_this(),
                    boost::asio::placeholders::error));
  }

 public:
  SSL_SOCKET::lowest_layer_type &get_socket() {
    return _socket->lowest_layer();
  }

  void connect(string ip, uint16_t port) {
    _ip = ip;
    _port = port;
    LOG_TRACE("enter, from: " << _selfId << " ,to: " << _destId
                              << ", ip: " << ip << ", port: " << port);

    tcp::endpoint ep(address::from_string(ip), port);
    LOG_DEBUG("connecting from: " << _selfId << " ,to: " << _destId
                                  << ", timeout: " << _currentTimeout);

    _connectTimer.expires_from_now(
        boost::posix_time::millisec(_currentTimeout));

    get_socket().async_connect(
        ep,
        boost::bind(&AsyncTlsConnection::connect_completed,
                    shared_from_this(),
                    boost::asio::placeholders::error));
    LOG_TRACE("exit, from: " << _selfId << " ,to: " << _destId << ", ip: " << ip
                             << ", port: " << port);
  }

  void start() {
    _socket->async_handshake(
        boost::asio::ssl::stream_base::server,
        boost::bind(&AsyncTlsConnection::handshake_completed,
                    this,
                    boost::asio::placeholders::error));
  }

  void send(const char *data, uint32_t length) {
    LOG_TRACE("enter, node " << _selfId << ", dest: " << _destId);

    lock_guard<recursive_mutex> lock(_connectionsGuard);
    auto offset = prepare_output_buffer(MessageType::Regular, length);
    memcpy(_outBuffer + offset, data, length);
    write_async(_outBuffer, offset + length);

    LOG_DEBUG("send exit, from: "
              << ", to: " << _destId << ", offset: " << offset
              << ", length: " << length);
    LOG_TRACE("exit, node " << _selfId << ", dest: " << _destId);
  }

  void setReceiver(NodeNum nodeId, IReceiver *rec) { _receiver = rec; }

  static ASYNC_CONN_PTR create(io_service *service,
                               function<void(NodeNum)> onError,
                               function<void(NodeNum, ASYNC_CONN_PTR)> onHello,
                               uint32_t bufferLength,
                               NodeNum destId,
                               NodeNum selfId,
                               string certificatesRootFolder,
                               ConnType type) {
    auto res = ASYNC_CONN_PTR(new AsyncTlsConnection(service,
                                                     onError,
                                                     onHello,
                                                     bufferLength,
                                                     destId,
                                                     selfId,
                                                     certificatesRootFolder,
                                                     type));
    res->init();
    return res;
  }

  virtual ~AsyncTlsConnection() {
    LOG_TRACE("enter, node " << _selfId << ", dest: " << _destId
                             << ", connected: " << connected
                             << ", closed: " << _closed);

    delete[] _inBuffer;
    delete[] _outBuffer;

    LOG_TRACE("exit, node " << _selfId << ", dest: " << _destId
                            << ", connected: " << connected
                            << ", closed: " << _closed);
  }
};

////////////////////////////////////////////////////////////////////////////
class TlsTCPCommunication::TlsTcpImpl {
 private:
  unordered_map<NodeNum, ASYNC_CONN_PTR> _connections;

  unique_ptr<tcp::acceptor> _pAcceptor = nullptr;
  std::thread *_pIoThread = nullptr;

  NodeNum _selfId;
  IReceiver *_pReceiver = nullptr;

  // NodeNum mapped to tuple<ip, port> //
  NodeMap _nodes;
  io_service _service;
  uint16_t _listenPort;
  string _listenIp;
  uint32_t _bufferLength;
  uint32_t _maxServerId;
  string _certRootFolder;

  void on_async_connection_error(NodeNum peerId) {
    LOG_ERROR("to: " << peerId);
    lock_guard<recursive_mutex> lock(_connectionsGuard);
    _connections.erase(peerId);
  }

  void on_hello_message(NodeNum id, ASYNC_CONN_PTR conn) {
    LOG_DEBUG("node: " << _selfId << ", from: " << id);

    lock_guard<recursive_mutex> lock(_connectionsGuard);
    conn->setReceiver(id, _pReceiver);
    _connections.insert(make_pair(id, conn));
  }

  void on_accept(ASYNC_CONN_PTR conn, const B_ERROR_CODE &ec) {
    LOG_TRACE("enter, node: " + to_string(_selfId) + ", ec: " + ec.message());

    if (!ec) {
      conn->connected = true;
      conn->start();
    }

    // LOG4CPLUS_DEBUG(logger_, "handle_accept before start_accept");
    start_accept();
    LOG_TRACE("exit, node: " + to_string(_selfId) + ", ec: " + ec.message());
  }

  // here need to check how "this" passed to handlers behaves if the object is
  // deleted.
  void start_accept() {
    LOG_TRACE("enter, node: " << _selfId);
    auto conn = AsyncTlsConnection::create(
        &_service,
        std::bind(&TlsTcpImpl::on_async_connection_error,
                  this,
                  std::placeholders::_1),
        std::bind(&TlsTcpImpl::on_hello_message,
                  this,
                  std::placeholders::_1,
                  std::placeholders::_2),
        _bufferLength,
        0,
        _selfId,
        _certRootFolder,
        ConnType::Incoming);
    _pAcceptor->async_accept(conn->get_socket().lowest_layer(),
                             boost::bind(&TlsTcpImpl::on_accept,
                                         this,
                                         conn,
                                         boost::asio::placeholders::error));
    LOG_TRACE("exit, node: " << _selfId);
  }

  TlsTcpImpl(const TlsTcpImpl &) = delete;
  TlsTcpImpl(const TlsTcpImpl &&) = delete;
  TlsTcpImpl &operator=(const TlsTcpImpl &) = delete;
  TlsTcpImpl() = delete;

  TlsTcpImpl(NodeNum selfNodeNum,
             NodeMap nodes,
             uint32_t bufferLength,
             uint16_t listenPort,
             uint32_t maxServerId,
             string listenIp,
             string certRootFolder)
      : _selfId(selfNodeNum),
        _listenPort(listenPort),
        _listenIp(listenIp),
        _bufferLength(bufferLength),
        _maxServerId(maxServerId),
        _certRootFolder(certRootFolder) {
    //_service = new io_service();
    for (auto it = nodes.begin(); it != nodes.end(); it++) {
      _nodes.insert({it->first, it->second});
    }

    if (_selfId < _maxServerId) {
      tcp::endpoint ep(address::from_string(_listenIp), _listenPort);
      _pAcceptor = boost::make_unique<tcp::acceptor>(_service, ep);
      start_accept();
    } else
      LOG_INFO("skipping listen for node: " << _selfId);

    for (auto it = _nodes.begin(); it != _nodes.end(); it++) {
      // connect only to nodes with ID higher than selfId
      // and all nodes with lower ID will connect to this node
      if (it->first < _selfId && it->first < maxServerId) {
        auto conn = AsyncTlsConnection::create(
            &_service,
            std::bind(&TlsTcpImpl::on_async_connection_error,
                      this,
                      std::placeholders::_1),

            std::bind(&TlsTcpImpl::on_hello_message,
                      this,
                      std::placeholders::_1,
                      std::placeholders::_2),
            _bufferLength,
            it->first,
            _selfId,
            _certRootFolder,
            ConnType::Outgoing);

        _connections.insert(make_pair(it->first, conn));
        string peerIp = std::get<0>(it->second);
        uint16_t peerPort = std::get<1>(it->second);
        conn->connect(peerIp, peerPort);
        LOG_TRACE("connect called for node " << to_string(it->first));
      }
    }
  }

 public:
  static TlsTcpImpl *create(NodeNum selfNodeId,
                            // tuple {ip, listen port}
                            NodeMap nodes,
                            uint32_t bufferLength,
                            uint16_t listenPort,
                            uint32_t tempHighestNodeForConnecting,
                            string listenIp,
                            string certRootFolder) {
    return new TlsTcpImpl(selfNodeId,
                          nodes,
                          bufferLength,
                          listenPort,
                          tempHighestNodeForConnecting,
                          listenIp,
                          certRootFolder);
  }

  int getMaxMessageSize() { return _bufferLength; }

  int Start() {
    if (_pIoThread) return 0;  // running

    _pIoThread = new std::thread(
        std::bind(static_cast<size_t (boost::asio::io_service::*)()>(
                      &boost::asio::io_service::run),
                  std::ref(_service)));

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
    _service.reset();

    return 0;
  }

  bool isRunning() const {
    if (!_pIoThread) return false;  // stopped
    return true;
  }

  ConnectionStatus getCurrentConnectionStatus(const NodeNum node) const {
    return isRunning() ? ConnectionStatus::Connected
                       : ConnectionStatus::Disconnected;
  }

  void setReceiver(NodeNum nodeId, IReceiver *rec) {
    _pReceiver = rec;
    for (auto it : _connections) {
      it.second->setReceiver(nodeId, rec);
    }
  }

  /**
   * Sends a message on the underlying communication layer to a given
   * destination node. Asynchronous (non-blocking) method.
   * Returns 0 on success.
   */
  int sendAsyncMessage(const NodeNum destNode,
                       const char *const message,
                       const size_t messageLength) {
    LOG_TRACE("enter, from: " << _selfId << ", to: " << to_string(destNode));

    lock_guard<recursive_mutex> lock(_connectionsGuard);
    auto temp = _connections.find(destNode);
    if (temp != _connections.end()) {
      LOG_TRACE("conncection found, from: " << _selfId << ", to: " << destNode);

      if (temp->second->connected) {
        temp->second->send(message, messageLength);
      } else
        LOG_TRACE("connecction found but disconnected, from: "
                  << _selfId << ", to: " << destNode);
    }

    LOG_TRACE("exit, from: " << _selfId << ", to: " << to_string(destNode));
    return 0;
  }

  ~TlsTcpImpl() { LOG_TRACE("DTOR!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"); }
};

TlsTCPCommunication::~TlsTCPCommunication() { _ptrImpl->Stop(); }

TlsTCPCommunication::TlsTCPCommunication(const TlsTcpConfig &config) {
  _ptrImpl = TlsTcpImpl::create(config.selfId,
                                config.nodes,
                                config.bufferLength,
                                config.listenPort,
                                config.maxServerId,
                                config.listenIp,
                                config.certificatesRootPath);
}

TlsTCPCommunication *TlsTCPCommunication::create(const TlsTcpConfig &config) {
  return new TlsTCPCommunication(config);
}

int TlsTCPCommunication::getMaxMessageSize() {
  return _ptrImpl->getMaxMessageSize();
}

int TlsTCPCommunication::Start() { return _ptrImpl->Start(); }

int TlsTCPCommunication::Stop() {
  if (!_ptrImpl) return 0;

  auto res = _ptrImpl->Stop();
  delete _ptrImpl;
  return res;
}

bool TlsTCPCommunication::isRunning() const { return _ptrImpl->isRunning(); }

ConnectionStatus TlsTCPCommunication::getCurrentConnectionStatus(
    const NodeNum node) const {
  return _ptrImpl->getCurrentConnectionStatus(node);
}

int TlsTCPCommunication::sendAsyncMessage(const NodeNum destNode,
                                          const char *const message,
                                          const size_t messageLength) {
  return _ptrImpl->sendAsyncMessage(destNode, message, messageLength);
}

void TlsTCPCommunication::setReceiver(NodeNum receiverNum,
                                      IReceiver *receiver) {
  _ptrImpl->setReceiver(receiverNum, receiver);
}
