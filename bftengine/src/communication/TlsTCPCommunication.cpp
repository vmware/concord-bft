// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the
// LICENSE file.

/**
 * This file implements the TLS over TCP communication between concord nodes.
 * There are 2 main classes: AsyncTlsConnection - that represents stateful
 * connection between 2 nodes and TlsTCPCommunication - that uses PIMPL idiom
 * to implement the ICommunication interface.
 * The AsyncTlsConnection uses boost::asio::io_service with 1 worker thread -
 * ensuring serial execution of the callbacks. The internal state variables,
 * _closed, _authenticated and _connected, are accessed from the callbacks
 * only - making them thread safe and eliminating need to synchronize the
 * access
 * */

#include "CommDefs.hpp"
#include <string>
#include <functional>
#include <iostream>
#include <sstream>
#include <thread>
#include <chrono>
#include <mutex>
#include <regex>
#include <cassert>
#include <deque>

#include "boost/bind.hpp"
#include <boost/asio.hpp>
#include <boost/make_unique.hpp>
#include <boost/asio/deadline_timer.hpp>
#include <boost/date_time/posix_time/posix_time_duration.hpp>
#include <boost/filesystem.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/range.hpp>
#include "openssl/ssl.h"
#include <openssl/x509.h>
#include <openssl/x509v3.h>
#include "Logging.hpp"

using namespace std;
using namespace concordlogger;
using namespace boost;

namespace bftEngine {

class AsyncTlsConnection;

typedef boost::system::error_code B_ERROR_CODE;
typedef std::shared_ptr<AsyncTlsConnection> ASYNC_CONN_PTR;
typedef asio::ssl::stream<asio::ip::tcp::socket> SSL_SOCKET;
typedef unique_ptr<SSL_SOCKET> B_TLS_SOCKET_PTR;

enum ConnType : uint8_t {
  NotDefined = 0,
  Incoming,
  Outgoing
};

/**
 * this class will handle single connection using boost::make_shared idiom
 * will receive the IReceiver as a parameter and call it when new message
 * available.
 * The class is responsible for managing connection lifecycle. If the
 * instance represents incoming connection, when the connection is broken the
 * instance will clean up itself by calling the _fOnError method.
 * The Outgoing connection instance will be disposed and the new one
 * will be created when connection is broken
 */
class AsyncTlsConnection : public
                           std::enable_shared_from_this<AsyncTlsConnection> {
 public:
  // since 0 is legal node number, we must initialize it to some "not
  // defined" value. This approach is fragile. TODO:(IG) use real "not
  // defined" value
  static const NodeNum UNKNOWN_NODE_ID = numeric_limits<NodeNum>::max();

 private:

  struct OutMessage {
    char* data = nullptr;
    size_t length = 0;

    OutMessage(char* msg, uint32_t msgLength) :
        data{msg},
        length{msgLength}
    {
    }

    OutMessage& operator=(OutMessage&& other) {
      if (this != &other) {
        if(data) {
          delete[] data;
        }
        data = other.data;
        length = other.length;
        other.data = nullptr;
        other.length = 0;
      }
      return *this;
    }

    OutMessage(OutMessage&& other) : data{nullptr}, length{0} {
      *this = std::move(other);
    };

    OutMessage& operator=(const OutMessage&) = delete;
    OutMessage(const OutMessage& other) = delete;

    ~OutMessage() {
      if(data) {
        delete[] data;
      }
    }
  };

  // msg header: 4 bytes msg length
  static constexpr uint8_t MSG_LENGTH_FIELD_SIZE = 4;
  static constexpr uint8_t MSG_HEADER_SIZE = MSG_LENGTH_FIELD_SIZE;

  // maybe need to define as a function of the input length per operation?
  static constexpr uint32_t WRITE_TIME_OUT_MILLI = 10000;
  static constexpr uint32_t READ_TIME_OUT_MILLI = 10000;

  bool _isReplica = false;
  bool _destIsReplica = false;
  asio::io_service *_service = nullptr;
  uint32_t _maxMessageLength;
  char *_inBuffer;
  IReceiver *_receiver = nullptr;
  std::function<void(NodeNum)> _fOnError = nullptr;
  std::function<void(NodeNum, ASYNC_CONN_PTR)> _fOnTlsReady = nullptr;
  NodeNum _expectedDestId = AsyncTlsConnection::UNKNOWN_NODE_ID;
  uint32_t _bufferLength;
  NodeNum _destId = AsyncTlsConnection::UNKNOWN_NODE_ID;
  NodeNum _selfId;
  string _ip = "";
  uint16_t _port = 0;
  asio::deadline_timer _connectTimer;
  asio::deadline_timer _writeTimer;
  asio::deadline_timer _readTimer;
  ConnType _connType;
  string _cipherSuite;
  uint16_t _minTimeout = 256;
  uint16_t _maxTimeout = 8192;
  uint16_t _currentTimeout = _minTimeout;
  B_TLS_SOCKET_PTR _socket = nullptr;
  string _certificatesRootFolder;
  Logger _logger;
  UPDATE_CONNECTIVITY_FN _statusCallback = nullptr;
  NodeMap _nodes;
  asio::ssl::context _sslContext;
  deque<OutMessage> _outQueue;
  mutex _writeLock;

  // internal state
  bool _disposed = false;
  bool _authenticated = false;
  bool _connected = false;
 public:

 private:
  AsyncTlsConnection(asio::io_service *service,
                     function<void(NodeNum)> onError,
                     function<void(NodeNum, ASYNC_CONN_PTR)> onAuthenticated,
                     uint32_t bufferLength,
                     NodeNum destId,
                     NodeNum selfId,
                     string certificatesRootFolder,
                     ConnType type,
                     NodeMap nodes,
                     string cipherSuite,
                     UPDATE_CONNECTIVITY_FN statusCallback = nullptr) :
      _service(service),
      _maxMessageLength(bufferLength + MSG_HEADER_SIZE + 1),
      _fOnError(onError),
      _fOnTlsReady(onAuthenticated),
      _expectedDestId(destId),
      _bufferLength(bufferLength),
      _selfId(selfId),
      _connectTimer(*service),
      _writeTimer(*service),
      _readTimer(*service),
      _connType(type),
      _cipherSuite(cipherSuite),
      _certificatesRootFolder(certificatesRootFolder),
      _logger(Logger::getLogger("concord-bft.tls")),
      _statusCallback{statusCallback},
      _nodes{std::move(nodes)},
      _sslContext{asio::ssl::context(type == ConnType::Incoming
                                     ? asio::ssl::context::tlsv12_server
                                     : asio::ssl::context::tlsv12_client)},
      _disposed(false),
      _authenticated{false},
      _connected{false} {
    LOG_DEBUG(_logger, "ctor, node " << _selfId << ", connType: " << _destId);

    _inBuffer = new char[_bufferLength];
    _connectTimer.expires_at(boost::posix_time::pos_infin);
    _writeTimer.expires_at(boost::posix_time::pos_infin);
    _readTimer.expires_at(boost::posix_time::pos_infin);
    _isReplica = check_replica(selfId);
  }

  /**
   * The set_tls method needs to be set outside of the ctor to allow
   * shared_from_this() usage. Socket creation must be done here, after set_tls
   */
  void init() {
    set_tls();
    _socket = B_TLS_SOCKET_PTR(new SSL_SOCKET(*_service, _sslContext));
  }

  void set_disposed(bool value) {
    _disposed = value;
  }

  void set_connected(bool value) {
    _connected = value;
  }

  void set_authenticated(bool value) {
    _authenticated = value;
  }

  bool check_replica(NodeNum node) {
    auto it = _nodes.find(node);
    if (it == _nodes.end()) {
      return false;
    }

    return it->second.isReplica;
  }

  /**
   * returns message length - first 4 bytes of the buffer
   * @param buffer Data received from the stream
   * @return Message length, 4 bytes long
   */
  uint32_t get_message_length(const char *buffer) {
    return *(reinterpret_cast<const uint32_t*>(buffer));
  }

  /// ****************** cleanup functions ******************** ///

  bool was_error(const B_ERROR_CODE &ec, string where) {
    if (ec) {
      LOG_ERROR(_logger,
                "was_error, where: " << where
                                     << ", node " << _selfId
                                     << ", dest: " << _destId
                                     << ", connected: " << _connected
                                     << ", ex: " << ec.message());
    }
    return (ec != 0);
  }

  /**
   * this method closes the socket and frees the object by calling the _fOnError
   * we rely on boost cleanup and do not shutdown ssl and sockets explicitly
   */
  void dispose_connection() {
    if (_disposed)
      return;

    set_authenticated(false);
    set_connected(false);
    set_disposed(true);

    LOG_DEBUG(_logger,
              "dispose_connection, node " << _selfId << ", dest: " << _destId
                                          << ", connected: " << _connected
                                          << ", closed: " << _disposed);

    _connectTimer.cancel();
    _writeTimer.cancel();
    _readTimer.cancel();

    _fOnError(_destId);
  }

  /**
   * generic error handling function
   */
  void handle_error() {
    assert(_connType != ConnType::NotDefined);

    if (_statusCallback) {
      bool isReplica = check_replica(_destId);
      if (isReplica) {
        PeerConnectivityStatus pcs{};
        pcs.peerId = _destId;
        pcs.statusType = StatusType::Broken;

        // pcs.statusTime = we dont set it since it is set by the aggregator
        // in the upcoming version timestamps should be reviewed
        _statusCallback(pcs);
      }
    }

    dispose_connection();
  }
  /// ****************** cleanup functions* end ******************* ///

  /// ****************** TLS related functions ******************** ///

  /**
   * tls handhshake callback for the client
   * @param error code
   */
  void on_handshake_complete_outbound(const B_ERROR_CODE &ec) {
    set_connected(true);
    // When authenticated, replace custom verification by default one.
    // This is mandatory to avoid leaking 'this' since the verification
    // callback was binded using shared_from_this and the SSL context
    // will retain the callback and thus 'this' will not be destroyed
    // when needed
    _sslContext.set_verify_callback([](bool p,
                                       asio::ssl::verify_context&) {
      return p;
    });
    bool err = was_error(ec, "on_handshake_complete_outbound");
    if (err) {
      handle_error();
      return;
    }

    set_authenticated(true);
    _connectTimer.expires_at(boost::posix_time::pos_infin);
    _currentTimeout = _minTimeout;
    assert(_destId == _expectedDestId);
    _fOnTlsReady(_destId, shared_from_this());
    read_msg_length_async();
  }

  /**
   * tls handhshake callback for the server
   * @param error code
   */
  void on_handshake_complete_inbound(const B_ERROR_CODE &ec) {
    set_connected(true);
    // When authenticated, replace custom verification by default one.
    // This is mandatory to avoid leaking 'this' since the verification
    // callback was binded using shared_from_this and the SSL context
    // will retain the callback and thus 'this' will not be destroyed
    // when needed
    _sslContext.set_verify_callback([](bool p,
                                       asio::ssl::verify_context&) {
      return p;
    });
    bool err = was_error(ec, "on_handshake_complete_inbound");
    if (err) {
      handle_error();
      return;
    }

    set_authenticated(true);
    // to match asserts over the code
    // in the incoming connection we don't know the expected peer id
    _expectedDestId = _destId;
    _fOnTlsReady(_destId, shared_from_this());
    read_msg_length_async();
  }

  void set_tls() {
    assert(_connType != ConnType::NotDefined);

    if (ConnType::Incoming == _connType)
      set_tls_server();
    else
      set_tls_client();

    // force specific suite/s only. we want to use the strongest cipher suites
    SSL_CTX_set_cipher_list(_sslContext.native_handle(), _cipherSuite.c_str());
  }

  void set_tls_server() {
    _sslContext.set_verify_mode(asio::ssl::verify_peer |
        asio::ssl::verify_fail_if_no_peer_cert);
    _sslContext.set_options(
        boost::asio::ssl::context::default_workarounds
            | boost::asio::ssl::context::no_sslv2
            | boost::asio::ssl::context::no_sslv3
            | boost::asio::ssl::context::no_tlsv1
            | boost::asio::ssl::context::no_tlsv1_1
            | boost::asio::ssl::context::single_dh_use);

    _sslContext.set_verify_callback(
        boost::bind(&AsyncTlsConnection::verify_certificate_server,
                    shared_from_this(),
                    _1,
                    _2));

    namespace fs = boost::filesystem;
    auto path = fs::path(_certificatesRootFolder) /
        fs::path(to_string(_selfId)) /
        fs::path("server");
    _sslContext.use_certificate_chain_file(
        (path / fs::path("server.cert")).string());
    _sslContext.use_private_key_file(
        (path / fs::path("pk.pem")).string(),
        boost::asio::ssl::context::pem);

    // if we cant create EC DH params, it may mean that SSL version old or
    // any other crypto related errors, we can't continue with TLS
    EC_KEY *ecdh = EC_KEY_new_by_curve_name(NID_secp384r1);
    if (!ecdh) {
      LOG_ERROR(_logger, "Unable to create EC");
      abort();
    }

    if (1 != SSL_CTX_set_tmp_ecdh (_sslContext.native_handle(), ecdh)) {
      LOG_ERROR(_logger, "Unable to set temp EC params");
      abort();
    }

    // as the OpenSSL does reference counting, it should be safe to free the key
    // however, no explicit info on this point in the openssl docs.
    // this info is from various online sources and examples
    EC_KEY_free(ecdh);
  }

  void set_tls_client() {
    _sslContext.set_verify_mode(asio::ssl::verify_peer);

    namespace fs = boost::filesystem;
    auto path = fs::path(_certificatesRootFolder) /
        fs::path(to_string(_selfId)) /
        "client";
    auto serverPath = fs::path(_certificatesRootFolder) /
        fs::path(to_string(_expectedDestId)) /
        "server";

    _sslContext.set_verify_callback(
        boost::bind(&AsyncTlsConnection::verify_certificate_client,
                    shared_from_this(),
                    _1,
                    _2));

    _sslContext.use_certificate_chain_file((path / "client.cert").string());
    _sslContext.use_private_key_file((path / "pk.pem").string(),
                                     boost::asio::ssl::context::pem);
  }

  bool verify_certificate_server(bool preverified,
                                 boost::asio::ssl::verify_context &ctx) {
    char subject[512];
    X509 *cert = X509_STORE_CTX_get_current_cert(ctx.native_handle());
    if (!cert) {
      LOG_ERROR(_logger, "no certificate from client");
      return false;
    } else {
      X509_NAME_oneline(X509_get_subject_name(cert), subject, 512);
      LOG_DEBUG(_logger,
                "Verifying client: " << subject << ", " << preverified);
      bool res = check_certificate(cert, "client", string(subject),
                                   UNKNOWN_NODE_ID);
      LOG_DEBUG(_logger,
                "Manual verifying client: " << subject
                                            << ", authenticated: " << res);
      return res;
    }
  }

  bool verify_certificate_client(bool preverified,
                                 boost::asio::ssl::verify_context &ctx) {
    char subject[256];
    X509 *cert = X509_STORE_CTX_get_current_cert(ctx.native_handle());
    if (!cert) {
      LOG_ERROR(_logger, "no certificate from server");
      return false;
    } else {
      X509_NAME_oneline(X509_get_subject_name(cert), subject, 256);
      LOG_DEBUG(_logger,
                "Verifying server: " << subject << ", " << preverified);

      bool res = check_certificate(
          cert, "server", string(subject), _expectedDestId);
      LOG_DEBUG(_logger,
                "Manual verifying server: " << subject
                                            << ", authenticated: " << res);
      return res;
    }
  }

  /**
   * certificate pinning
   * check for specific certificate and do not rely on the chain authentication
   * if verified, it sets explicitly the _destId
   * When used on the server, the expectedPeerId will always be UNKNOWN_PEER_ID
   * and there is no check whether the remote peer id is equal to expected
   * peer id
   */
  bool check_certificate(X509 *receivedCert,
                         string connectionType,
                         string subject,
                         NodeNum expectedPeerId) {
    // first, basic sanity test, just to eliminate disk read if the certificate
    // is unknown.
    // the certificate must have node id, as we put it in OU field on creation.
    // since we use pinning we must know who is the remote peer.
    // peerIdPrefixLength stands for the length of 'OU=' substring
    size_t peerIdPrefixLength = 3;
    std::regex r("OU=\\d*", std::regex_constants::icase);
    std::smatch sm;
    regex_search(subject, sm, r);
    if (sm.length() <= peerIdPrefixLength) {
      LOG_ERROR(_logger, "OU not found or empty: " << subject);
      return false;
    }

    string remPeer =
        sm.str().substr(
            peerIdPrefixLength, sm.str().length() - peerIdPrefixLength);
    if (0 == remPeer.length()) {
      LOG_ERROR(_logger, "OU empty " << subject);
      return false;
    }

    NodeNum remotePeerId;
    try {
      remotePeerId = stoul(remPeer, nullptr);
    } catch (const std::invalid_argument &ia) {
      LOG_ERROR(_logger, "cannot convert OU, " << subject << ", " << ia.what());
      return false;
    } catch (const std::out_of_range &e) {
      LOG_ERROR(_logger, "cannot convert OU, " << subject << ", " << e.what());
      return false;
    }

    // if server has been verified, check peers match
    if (UNKNOWN_NODE_ID != expectedPeerId) {
      if (remotePeerId != expectedPeerId) {
        LOG_ERROR(_logger, "peers doesnt match, expected: " << expectedPeerId
                                                            << ", received: "
                                                            << remPeer);
        return false;
      }
    }

    // the actual pinning - read the correct certificate from the disk and
    // compare it to the received one
    namespace fs = boost::filesystem;
    auto path = fs::path(_certificatesRootFolder) / to_string(remotePeerId)
        / connectionType
        / string(connectionType + ".cert");

    FILE *fp = fopen(path.c_str(), "r");
    if (!fp) {
      LOG_ERROR(_logger, "certificate file not found, path: " << path);
      return false;
    }

    X509 *localCert = PEM_read_X509(fp, NULL, NULL, NULL);
    if (!localCert) {
      LOG_ERROR(_logger, "cannot parse certificate, path: " << path);
      fclose(fp);
      return false;
    }

    // this is actual comparison, compares hash of 2 certs
    int res = X509_cmp(receivedCert, localCert);
    if (res == 0) {
      if (_destId == AsyncTlsConnection::UNKNOWN_NODE_ID) {
        LOG_INFO(_logger,
                 "connection authenticated, node: " << _selfId << ", type: " << _connType << ", expected peer: " << _expectedDestId
                                                    << ", peer: "
                                                    << remotePeerId);
      }

      if(_destId == UNKNOWN_NODE_ID) {
        _destId = remotePeerId;
      }
      _destIsReplica = check_replica(remotePeerId);
    }

    X509_free(localCert);
    fclose(fp);

    return res == 0;
  }
  /// ****************** TLS related functions end ******************** ///

  /// ************ connect functions ************************** ///

  /**
   * This function sets the time to wait before the next connection attempt.
   * The timeouts move from 256ms to 8s, multiplied by 2, and then stay at 8s.
   * The rationale here is to try to connect fast at the beginning (4
   * connection attempts in 1 sec) and then if failed probably the peer is
   * not ready and we don't want to try at the same rate introducing overhead
   * for Asio. This logic can be changed in future.
   */
  void set_timeout() {
    _currentTimeout = _currentTimeout == _maxTimeout
                      ? _maxTimeout
                      : _currentTimeout * 2;
  }

  /**
   * This is timer tick handler, if we are here either the timer was
   * cancelled and ec == aborted or the timer has reached the deadline.
   * @param ec
   */
  void connect_timer_tick(const B_ERROR_CODE &ec) {
    if (_disposed || ec == asio::error::operation_aborted) {
      return;
    }

    // deadline reached, try to reconnect
    connect(_ip, _port);
    LOG_DEBUG(_logger, "connect_timer_tick, node " << _selfId
                                                   << ", dest: " << _expectedDestId
                                                   << ", ec: " << ec.message());
  }

  /**
   * occures when async connect completes - need to check the socket & timer
   * states to determine timeout or conenection success
   * @param err
   */
  void connect_completed(const B_ERROR_CODE &err) {
    if (_disposed) {
      return;
    }

    auto res = was_error(err, __func__);

    if (res) {
      // async_connect opens socket on start so we need to close the socket if
      // not connected
      get_socket().close();
      set_connected(false);
      set_timeout();
      _connectTimer.expires_from_now(
          boost::posix_time::millisec(_currentTimeout));
      _connectTimer.async_wait(
          boost::bind(&AsyncTlsConnection::connect_timer_tick,
                      shared_from_this(),
                      boost::asio::placeholders::error));
    } else {
      set_connected(true);
      _connectTimer.cancel();
      LOG_DEBUG(_logger, "connected, node " << _selfId
                                            << ", dest: " << _expectedDestId
                                            << ", res: " << res);

      _socket->async_handshake(boost::asio::ssl::stream_base::client,
                               boost::bind(
                                   &AsyncTlsConnection::on_handshake_complete_outbound,
                                   shared_from_this(),
                                   boost::asio::placeholders::error));

    }

    LOG_TRACE(_logger, "exit, node " << _selfId << ", dest: " << _destId);
  }

  /// ************ connect functions end ************************** ///

  /// ************* read functions ******************* ////
  /// the read process is as follows:
  /// 1. read_some of MSG_HEADER_SIZE bytes without starting timer. This
  /// is async call and the callback can be triggered after partial read
  /// 2. start async_read of either the rest part of the header (if was
  /// partial) or the message itself. Start timer for handling the timeout.
  /// 3. when read completed, cancel the timer
  /// 4. if timer ticks - the read hasnt completed, close the connection.

  void on_read_timer_expired(const B_ERROR_CODE &ec) {
    if(_disposed) {
      return;
    }
    // check if the handle is not a result of calling expire_at()
    if(ec != boost::asio::error::operation_aborted) {
      dispose_connection();
    }
  }

  /**
  * occurs when some of msg length bytes are read from the stream
  * @param ec Error code
  * @param bytesRead actual bytes read
  */
  void
  read_msglength_completed(const B_ERROR_CODE &ec,
                           const uint32_t bytesRead,
                           bool first) {
    // if first is true - we came from partial reading, no timer was started
    if(!first) {
      auto res = _readTimer.expires_at(boost::posix_time::pos_infin);
      assert(res < 2); //can cancel at most 1 pending async_wait
    }
    if (_disposed) {
      return;
    }

    auto err = was_error(ec, __func__);
    if (err) {
      handle_error();
      return;
    }

    // if partial read of msg length bytes, continue
    if(first && bytesRead < MSG_LENGTH_FIELD_SIZE) {
      asio::async_read(
          *_socket,
          asio::buffer(_inBuffer + bytesRead, MSG_LENGTH_FIELD_SIZE - bytesRead),
          boost::bind(&AsyncTlsConnection::read_msglength_completed,
                      shared_from_this(),
                      boost::asio::placeholders::error,
                      boost::asio::placeholders::bytes_transferred,
                      false));
    } else { // start reading completely the whole message
      uint32_t msgLength = get_message_length(_inBuffer);
      if(msgLength == 0 || msgLength > _maxMessageLength - 1 - MSG_HEADER_SIZE){
        handle_error();
        return;
      }
      read_msg_async(msgLength);
    }

    auto res = _readTimer.expires_from_now(
        boost::posix_time::milliseconds(READ_TIME_OUT_MILLI));
    assert(res == 0); //can cancel at most 1 pending async_wait
    _readTimer.async_wait(
        boost::bind(&AsyncTlsConnection::on_read_timer_expired,
                    shared_from_this(),
                    boost::asio::placeholders::error));

    LOG_DEBUG(_logger, "exit, node " << _selfId
                                     << ", dest: " << _destId
                                     << ", connected: " << _connected
                                     << "is_open: " << get_socket().is_open());
  }

  /**
   * start reading message length bytes from the stream
   */
  void read_msg_length_async() {
    if (_disposed)
      return;

    // since we allow partial reading here, we dont need timeout
    _socket->async_read_some(
        asio::buffer(_inBuffer, MSG_LENGTH_FIELD_SIZE),
        boost::bind(&AsyncTlsConnection::read_msglength_completed,
                    shared_from_this(),
                    boost::asio::placeholders::error,
                    boost::asio::placeholders::bytes_transferred,
                    true));

    LOG_DEBUG(_logger,
              "read_msg_length_async, node " << _selfId
                                             << ", dest: " << _destId
                                             << ", connected: " << _connected
                                             << "is_open: " << get_socket().is_open());
  }

  /**
   * occurs when the whole message has been read from the stream
   * @param ec error code
   * @param bytesRead  actual bytes read
   */
  void read_msg_async_completed(const boost::system::error_code &ec,
                                size_t bytesRead) {
    auto res = _readTimer.expires_at(boost::posix_time::pos_infin);
    assert(res < 2); //can cancel at most 1 pending async_wait
    if (_disposed) {
      return;
    }

    if (_disposed) {
      return;
    }

    auto err = was_error(ec, __func__);
    if (err) {
      handle_error();
      return;
    }

    assert(_destId == _expectedDestId);
    try {
      if (_receiver) {
        _receiver->onNewMessage(_destId, _inBuffer, bytesRead);
      }
    } catch (std::exception &e) {
      LOG_ERROR(_logger, "read_msg_async_completed, exception:" << e.what());
    }

    read_msg_length_async();

    if (_statusCallback && _destIsReplica) {
      PeerConnectivityStatus pcs{};
      pcs.peerId = _destId;
      pcs.peerIp = _ip;
      pcs.peerPort = _port;
      pcs.statusType = StatusType::MessageReceived;

      // pcs.statusTime = we dont set it since it is set by the aggregator
      // in the upcoming version timestamps should be reviewed
      _statusCallback(pcs);
    }

    LOG_TRACE(_logger, "exit, node " << _selfId << ", dest: " << _destId);
  }

  /**
   * start reading message bytes after the length header has been read
   * @param msgLength
   */
  void read_msg_async(uint32_t msgLength) {
    if (_disposed) {
      return;
    }

    LOG_DEBUG(_logger, "read_msg_async, node " << _selfId << ", dest: " <<
                                               _destId);

    // async operation will finish when either expectedBytes are read
    // or error occured, this is what Asio guarantees
    async_read(*_socket,
               boost::asio::buffer(_inBuffer, msgLength),
               boost::bind(&AsyncTlsConnection::read_msg_async_completed,
                           shared_from_this(),
                           boost::asio::placeholders::error,
                           boost::asio::placeholders::bytes_transferred));

  }

  /// ************* read functions end ******************* ////

  /// ************* write functions ******************* ////
  /// the write process works as follows:
  /// 1. the send() function copies the data to the outgoing queue and calls
  /// post() - because the async_write_* should be called from one of the asio's
  /// worker threads.
  /// 2. the post callback checks if there is no pending write (queue size is 0
  /// and if true start asycn_write, with timer enabled
  /// 3. when write completed, cancels the timer. check if more messages
  /// are in the out queue - if true, start another async_write with timer.
  /// 4. if timer ticks - the write hasn't completed, close the connection.

  void put_message_header(char *data, uint32_t dataLength) {
    memcpy(data, &dataLength, MSG_LENGTH_FIELD_SIZE);
  }

  /**
   * If the timer tick occurs - shut down the connection.
   * Probably we need to find the better way to handle timeouts but the
   * current implementation assumes long enough timeouts to allow to write data
   * So if the timer occurs before the async write completes - we have a problem
   * @param ec Error code
   */
  void on_write_timer_expired(const B_ERROR_CODE &ec) {
    if(_disposed) {
      return;
    }
    // check if we the handle is not a result of calling expire_at()
    if(ec != boost::asio::error::operation_aborted) {
      dispose_connection();
    }
  }

  void start_async_write() {
    asio::async_write(
        *_socket,
        asio::buffer(_outQueue.front().data, _outQueue.front().length),
        boost::bind(
            &AsyncTlsConnection::async_write_complete,
            shared_from_this(),
            boost::asio::placeholders::error,
            boost::asio::placeholders::bytes_transferred));

    // start the timer to handle the write timeout
    auto res = _writeTimer.expires_from_now(
        boost::posix_time::milliseconds(WRITE_TIME_OUT_MILLI));
    assert(res == 0); //should not cancel any pending async wait
    _writeTimer.async_wait(
        boost::bind(&AsyncTlsConnection::on_write_timer_expired,
                    shared_from_this(),
                    boost::asio::placeholders::error));
  }

  /**
   * completion callback for the async write operation
   */
  void async_write_complete(const B_ERROR_CODE &ec, size_t bytesWritten) {
    auto res = _writeTimer.expires_at(boost::posix_time::pos_infin);
    assert(res < 2); //can cancel at most 1 pending async_wait
    if(_disposed) {
      return;
    }
    bool err = was_error(ec, "async_write_complete");
    if(err) {
      dispose_connection();
      return;
    }

    lock_guard<mutex> l(_writeLock);
    //remove the message that has been sent
    _outQueue.pop_front();

    // if there are more messages, continue to send but don' renmove, s.t.
    // the send() method will not trigger concurrent write
    if(_outQueue.size() > 0) {
      start_async_write();
    }
  }

  /**
   * start the async write operation
   * @param data
   * @param length
   */
  void do_write() {
    if (_disposed) {
      return;
    }

    //
    lock_guard<mutex> l(_writeLock);
    if(_outQueue.size() > 0) {
      start_async_write();
    }
  }

  /// ************* write functions end ******************* ////

 public:
  SSL_SOCKET::lowest_layer_type &get_socket() {
    return _socket->lowest_layer();
  }

  /**
   * start connection to the remote peer (Outgoing connection)
   * @param ip remote IP
   * @param port remote port
   * @param isReplica whether the peer is replica or client
   */
  void connect(string ip, uint16_t port) {
    _ip = ip;
    _port = port;

    asio::ip::tcp::endpoint ep(asio::ip::address::from_string(ip), port);

    get_socket().
        async_connect(ep,
                      boost::bind(&AsyncTlsConnection::connect_completed,
                                  shared_from_this(),
                                  boost::asio::placeholders::error));
    LOG_TRACE(_logger, "exit, from: " << _selfId
                                      << " ,to: " << _expectedDestId
                                      << ", ip: " << ip
                                      << ", port: " << port);
  }

  void start() {
    _socket->async_handshake(boost::asio::ssl::stream_base::server,
                             boost::bind(&AsyncTlsConnection::on_handshake_complete_inbound,
                                         shared_from_this(),
                                         boost::asio::placeholders::error));
  }

  /**
   * mimics the async sending by using Post to Asio working thread
   * this function posts the send request to the asio io service and then it
   * is executed in the worker thread.
   * @param data data to be sent
   * @param length data length
   */
  void send(const char *data, uint32_t length) {
    assert(data);
    assert(length > 0 && length <= _maxMessageLength - MSG_HEADER_SIZE);

    char *buf = new char[length + MSG_HEADER_SIZE];
    memset(buf, 0, length + MSG_HEADER_SIZE);
    put_message_header(buf, length);
    memcpy(buf + MSG_HEADER_SIZE, data, length);

    // here we lock to protect multiple thread access and to synch with callback
    // queue access
    lock_guard<mutex> l(_writeLock);

    // push to the output queue
    OutMessage out = OutMessage(buf, length + MSG_HEADER_SIZE);
    _outQueue.push_back(std::move(out));

    // if there is only one message in the queue there are no pending writes
    // - we can start one
    // we must post to asio service because async operations should be
    // started from asio threads and not during pending async read
    if(_outQueue.size() == 1) {
      _service->post(boost::bind(&AsyncTlsConnection::do_write,
                                 shared_from_this()));
    }

    LOG_DEBUG(_logger, "from: " << _selfId
                                << ", to: " << _destId
                                << ", length: " << length);

    if (_statusCallback && _isReplica) {
      PeerConnectivityStatus pcs{};
      pcs.peerId = _selfId;
      pcs.statusType = StatusType::MessageSent;

      // pcs.statusTime = we dont set it since it is set by the aggregator
      // in the upcoming version timestamps should be reviewed
      _statusCallback(pcs);
    }
  }

  void setReceiver(NodeNum nodeId, IReceiver *rec) {
    _receiver = rec;
  }

  static ASYNC_CONN_PTR create(asio::io_service *service,
                               function<void(NodeNum)> onError,
                               function<void(NodeNum, ASYNC_CONN_PTR)> onReady,
                               uint32_t bufferLength,
                               NodeNum destId,
                               NodeNum selfId,
                               string certificatesRootFolder,
                               ConnType type,
                               UPDATE_CONNECTIVITY_FN statusCallback,
                               NodeMap nodes,
                               string cipherSuite) {
    auto res = ASYNC_CONN_PTR(
        new AsyncTlsConnection(service,
                               onError,
                               onReady,
                               bufferLength,
                               destId,
                               selfId,
                               certificatesRootFolder,
                               type,
                               nodes,
                               cipherSuite,
                               statusCallback));
    res->init();
    return res;
  }

  virtual ~AsyncTlsConnection() {
    LOG_INFO(_logger, "Dtor called, node: " << _selfId << "peer: " << _destId << ", type: " <<
                                            _connType);

    delete[] _inBuffer;

    _receiver = nullptr;
    _fOnError = nullptr;
    _fOnTlsReady = nullptr;
  }
};

////////////////////////////////////////////////////////////////////////////

/**
 * Implementation class. Is reponsible for creating listener on given port,
 * outgoing connections to the lower Id peers and accepting connections from
 *  higher ID peers.
 *  This is default behavior given the clients will always have higher IDs
 *  from the replicas. In this way we assure that clients will not connect to
 *  each other.
 */
class TlsTCPCommunication::TlsTcpImpl :
    public std::enable_shared_from_this<TlsTcpImpl> {
 private:
  unordered_map<NodeNum, ASYNC_CONN_PTR> _connections;

  unique_ptr<asio::ip::tcp::acceptor> _pAcceptor = nullptr;
  std::thread *_pIoThread = nullptr;

  NodeNum _selfId;
  IReceiver *_pReceiver = nullptr;

  // NodeNum mapped to tuple<ip, port> //
  NodeMap _nodes;
  asio::io_service _service;
  uint16_t _listenPort;
  string _listenIp;
  uint32_t _bufferLength;
  uint32_t _maxServerId;
  string _certRootFolder;
  Logger _logger;
  UPDATE_CONNECTIVITY_FN _statusCallback;
  string _cipherSuite;

  mutex _connectionsGuard;
  mutable mutex _startStopGuard;

  /**
   * When the connection is broken, this method is called  and the broken
   * connection is removed from the map. If the closed connection was
   * Outcoming this method will initiate a new one.
   * @param peerId ID of the remote peer for the failed connection.
   */
  void on_async_connection_error(NodeNum peerId) {
    LOG_DEBUG(_logger, "on_async_connection_error, peerId: " << peerId);
    lock_guard<mutex> lock(_connectionsGuard);
    if (_connections.find(peerId) != _connections.end()) {
      _connections.erase(peerId);
    }

    // here we check in the nodes map for the peer info and connect again, if
    // needed.
    auto iter = _nodes.find(peerId);
    if(iter != _nodes.end()) {
      if (iter->first < _selfId && iter->first <= _maxServerId) {
        create_outgoing_connection(peerId,
                                   iter->second.ip,
                                   iter->second.port);
      }
    } else {
      LOG_ERROR(_logger, "Unknown peer, id: " << peerId);
    }
  }

  /**
   * This function is called when the connection is authenticated and the TLS
   * handshake is done. From this point this connections is secured and can
   * be used by the application. Note, that if we already have the connection
   * from the same peer in the map we reject BOTH (e.g. malicious replica
   * that uses someone's else ID)
   * @param id
   * @param conn
   */
  void on_connection_authenticated(NodeNum id, ASYNC_CONN_PTR conn) {
    lock_guard<mutex> lock(_connectionsGuard);
    // probably bad replica?? TODO: think how to handle it in a better way
    // for now, just throw away both existing and a new one
    if (_connections.find(id) != _connections.end()) {
      LOG_ERROR(_logger, "new incoming connection with peer id that already "
                         "exists, destroying both, peer: " << id);
      _connections.erase(id);
      return;
    }

    conn->setReceiver(id, _pReceiver);
    _connections.insert(make_pair(id, conn));
  }

  void on_accept(ASYNC_CONN_PTR conn,
                 const B_ERROR_CODE &ec) {
    LOG_DEBUG(_logger, "on_accept, enter, node: " + to_string(_selfId) +
        ", ec: " + ec.message());

    if (!ec) {
      conn->start();
    }

    // When io_service is stopped, the handlers are destroyed and when the
    // io_service dtor runs they will be invoked with operation_aborted error.
    // In this case we dont want to listen again and we rely on the
    // shared_from_this for the cleanup.
    if(ec != asio::error::operation_aborted) {
      start_accept();
    }
  }

  // here need to check how "this" passed to handlers behaves if the object is
  // deleted.
  void start_accept() {
    LOG_DEBUG(_logger, "start_accept, node: " << _selfId);
    auto conn =
        AsyncTlsConnection::create(
            &_service,
            std::bind(
                &TlsTcpImpl::on_async_connection_error,
                shared_from_this(),
                std::placeholders::_1),
            std::bind(
                &TlsTcpImpl::on_connection_authenticated,
                shared_from_this(),
                std::placeholders::_1,
                std::placeholders::_2),
            _bufferLength,
            AsyncTlsConnection::UNKNOWN_NODE_ID,
            _selfId,
            _certRootFolder,
            ConnType::Incoming,
            _statusCallback,
            _nodes,
            _cipherSuite);
    _pAcceptor->async_accept(conn->get_socket().lowest_layer(),
                             boost::bind(
                                 &TlsTcpImpl::on_accept,
                                 shared_from_this(),
                                 conn,
                                 boost::asio::placeholders::error));
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
             string certRootFolder,
             string cipherSuite,
             UPDATE_CONNECTIVITY_FN statusCallback = nullptr) :
      _selfId(selfNodeNum),
      _listenPort(listenPort),
      _listenIp(listenIp),
      _bufferLength(bufferLength),
      _maxServerId(maxServerId),
      _certRootFolder(certRootFolder),
      _logger(Logger::getLogger("concord.tls")),
      _statusCallback{statusCallback},
      _cipherSuite{cipherSuite} {
    //_service = new io_service();
    for (auto it = nodes.begin(); it != nodes.end(); it++) {
      _nodes.insert({it->first, it->second});
    }
  }

  void create_outgoing_connection(
      NodeNum nodeId, string peerIp, uint16_t peerPort) {
    auto conn =
        AsyncTlsConnection::create(
            &_service,
            std::bind(&TlsTcpImpl::on_async_connection_error,
                      shared_from_this(),
                      std::placeholders::_1),

            std::bind(&TlsTcpImpl::on_connection_authenticated,
                      shared_from_this(),
                      std::placeholders::_1,
                      std::placeholders::_2),
            _bufferLength,
            nodeId,
            _selfId,
            _certRootFolder,
            ConnType::Outgoing,
            _statusCallback,
            _nodes,
            _cipherSuite);

    conn->connect(peerIp, peerPort);
    LOG_INFO(_logger, "connect called for node " << _selfId << ", dest: " << nodeId);
  }

 public:
  static std::shared_ptr<TlsTcpImpl> create(NodeNum selfNodeId,
                            NodeMap nodes,
                            uint32_t bufferLength,
                            uint16_t listenPort,
                            uint32_t tempHighestNodeForConnecting,
                            string listenIp,
                            string certRootFolder,
                            string cipherSuite,
                            UPDATE_CONNECTIVITY_FN statusCallback) {
    return std::shared_ptr<TlsTcpImpl>(new TlsTcpImpl(selfNodeId,
                          nodes,
                          bufferLength,
                          listenPort,
                          tempHighestNodeForConnecting,
                          listenIp,
                          certRootFolder,
                          cipherSuite,
                          statusCallback));
  }

  int getMaxMessageSize() {
    return _bufferLength;
  }

  /**
   * logics moved from the ctor to this method to allow shared_from_this
   * @return
   */
  int Start() {
    lock_guard<mutex> l(_startStopGuard);

    if (_pIoThread) {
      return 0; // running
    }

    // all replicas are in listen mode
    if (_selfId <= _maxServerId) {
      // patch, we need to listen to all interfaces in order to support
      // machines with internal/external IPs. Need to add "listen IP" to the BFT
      // config file.
      asio::ip::tcp::endpoint ep(
          asio::ip::address::from_string(_listenIp), _listenPort);
      _pAcceptor = boost::make_unique<asio::ip::tcp::acceptor>(_service, ep);
      start_accept();
    } else // clients don't listen
    LOG_INFO(_logger, "skipping listen for node: " << _selfId);

    // this node should connect only to nodes with lower ID
    // and all nodes with higher ID will connect to this node
    // we don't want that clients will connect to other clients
    for (auto it = _nodes.begin(); it != _nodes.end(); it++) {
      // connect only to nodes with ID higher than selfId
      // and all nodes with lower ID will connect to this node
      if (it->first < _selfId && it->first <= _maxServerId) {
        create_outgoing_connection(it->first, it->second.ip, it->second.port);
      }
    }

    _pIoThread =
        new std::thread(std::bind
                            (static_cast<size_t(boost::asio::io_service::*)()>
                             (&boost::asio::io_service::run),
                             std::ref(_service)));

    return 0;
  }

  /**
  * Stops the object (including its internal threads).
  * On success, returns 0.
  */
  int Stop() {
    lock_guard<mutex> l(_startStopGuard);

    if (!_pIoThread) {
      return 0; // stopped
    }

    _service.stop();
    _pIoThread->join();

    _connections.clear();

    return 0;
  }

  bool isRunning() const {
    lock_guard<mutex> l(_startStopGuard);

    if (!_pIoThread) {
      return false; // stopped
    }

    return true;
  }

  ConnectionStatus
  getCurrentConnectionStatus(const NodeNum node) const {
    return isRunning() ? ConnectionStatus::Connected :
           ConnectionStatus::Disconnected;
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
    lock_guard<mutex> lock(_connectionsGuard);
    auto temp = _connections.find(destNode);
    if (temp != _connections.end()) {
      temp->second->send(message, messageLength);
    } else {
      LOG_DEBUG(_logger,
                "connection NOT found, from: " << _selfId
                                               << ", to: " << destNode);
    }

    return 0;
  }

  ~TlsTcpImpl() {
    LOG_DEBUG(_logger, "TlsTCPDtor");
    _pIoThread = nullptr;
  }
};

TlsTCPCommunication::~TlsTCPCommunication() {

}

TlsTCPCommunication::TlsTCPCommunication(const TlsTcpConfig &config) {
  _ptrImpl = TlsTcpImpl::create(config.selfId,
                                config.nodes,
                                config.bufferLength,
                                config.listenPort,
                                config.maxServerId,
                                config.listenIp,
                                config.certificatesRootPath,
                                config.cipherSuite,
                                config.statusCallback);
}

TlsTCPCommunication *TlsTCPCommunication::create(const TlsTcpConfig &config) {
  return new TlsTCPCommunication(config);
}

int TlsTCPCommunication::getMaxMessageSize() {
  return _ptrImpl->getMaxMessageSize();
}

int TlsTCPCommunication::Start() {
  return _ptrImpl->Start();
}

int TlsTCPCommunication::Stop() {
  if (!_ptrImpl)
    return 0;

  auto res = _ptrImpl->Stop();
  return res;
}

bool TlsTCPCommunication::isRunning() const {
  return _ptrImpl->isRunning();
}

ConnectionStatus
TlsTCPCommunication::getCurrentConnectionStatus(const NodeNum node) const {
  return _ptrImpl->getCurrentConnectionStatus(node);
}

int
TlsTCPCommunication::sendAsyncMessage(const NodeNum destNode,
                                      const char *const message,
                                      const size_t messageLength) {
  return _ptrImpl->sendAsyncMessage(destNode, message, messageLength);
}

void
TlsTCPCommunication::setReceiver(NodeNum receiverNum, IReceiver *receiver) {
  _ptrImpl->setReceiver(receiverNum, receiver);
}
} // namespace bftEngine
