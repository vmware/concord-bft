// Concord
//
// Copyright (c) 2020-2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#ifndef THIN_REPLICA_CLIENT_TRS_CONNECTION_HPP_
#define THIN_REPLICA_CLIENT_TRS_CONNECTION_HPP_

#include <algorithm>
#include <fstream>
#include <sstream>

#include <grpcpp/grpcpp.h>
#include <log4cplus/loggingmacros.h>
#include "thin_replica.grpc.pb.h"

using namespace std::chrono_literals;

namespace thin_replica_client {

struct TrsConnectionConfig {
  // thin_replica_tls_cert_path specifies the path of the certificates used for
  // the TLS channel.
  const std::string thin_replica_tls_cert_path;
  // is_insecure_trc_val determines if TLS enabled secure channel will be opened
  // by the thin replica client, or an insecure channel will be employed.
  const std::string is_insecure_trc_val;
  // client_key is the private key used by TRC to create a secure channel if
  // `is_insecure_trc_val` is false.
  const std::string client_key;
  const std::string client_cert_path;
  const std::string server_cert_path;

  TrsConnectionConfig(const std::string& thin_replica_tls_cert_path_,
                      const std::string& is_insecure_trc_val_,
                      const std::string& client_key_)
      : thin_replica_tls_cert_path(thin_replica_tls_cert_path_),
        is_insecure_trc_val(is_insecure_trc_val_),
        client_key(client_key_),
        client_cert_path(thin_replica_tls_cert_path + "/client.cert"),
        server_cert_path(thin_replica_tls_cert_path + "/server.cert") {}
};

// The default message size for incoming data is 4MiB but certain workloads
// demand a higher limit. With the tested workloads, the incoming message size
// from the TRS is less than 16MiB. This correlates with the maximum message
// size that we specify for the SBFT protocol in Concord's configuration file.
// Note: We can set the upper bound to unlimited (-1) but we do want to know
// when & why the message size increases.
const int kGrpcMaxInboundMsgSizeInBytes = 1 << 24;

// Class for managing a connection from a Thin Replica Client to a Thin Replica
// Server, abstracting the connection and communication implementations from the
// core Thin Replica Client logic in thin_replica_client::ThinReplicaClient.
//
// The TrsConnection itself provides the production implementation used for
// communication by the Thin Replica Client Library, however, TrsConnection also
// defines the interface (with its virtual functions) that the ThinReplicaClient
// implementation abstractly uses for communication, so it can be extended in
// order to use an alternative connection and communication implementation (for
// example, for testing).
//
// TrsConnection provides NO thread safety guarantees in the event more than one
// call to a TrsConnection function executes concurrently on the same
// TrsConnection object.
class TrsConnection {
 public:
  // Possible results of RPC operations a TrsConnection may report.
  enum class Result { kSuccess, kFailure, kTimeout };

  TrsConnection(const std::string& address,
                const std::string& client_id,
                uint16_t data_operation_timeout_seconds,
                uint16_t hash_operation_timeout_seconds)
      : logger_(log4cplus::Logger::getInstance("thin_replica_client.trsconn")),
        address_(address),
        client_id_(client_id),
        data_timeout_(std::chrono::seconds(data_operation_timeout_seconds)),
        hash_timeout_(std::chrono::seconds(hash_operation_timeout_seconds)) {}
  virtual ~TrsConnection() { this->disconnect(); }

  // Connect & disconnect from the TRS
  virtual void connect(std::unique_ptr<TrsConnectionConfig>& config);
  virtual bool isConnected();
  virtual void disconnect();

  // Open a data subscription stream (connection has to be established before).
  // A data subscription stream will be open after openDataStream returns if and
  // only if openDataStream returns Result::kSuccess; a stream will not be open
  // in the failure and timeout cases.
  virtual Result openDataStream(const com::vmware::concord::thin_replica::SubscriptionRequest& request);

  virtual void cancelDataStream();
  virtual bool hasDataStream();

  // Read data from an existing open data subscription stream. Note readData
  // will automatically close the open data stream in the event it has to time
  // out the read.
  virtual Result readData(com::vmware::concord::thin_replica::Data* data);

  // Open a hash subscription stream (connection has to be established before).
  // A hash subscription stream will be open after openHashStream returns if and
  // only if openDataStream returns Result::kSuccess; a stream will not be open
  // in the failure and timeout cases.
  virtual Result openHashStream(com::vmware::concord::thin_replica::SubscriptionRequest& request);

  virtual void cancelHashStream();
  virtual bool hasHashStream();

  // Read a hash from an existing open hash subscription stream. Note readHash
  // will automatically close the open hash stream in the event it has to time
  // out the read.
  virtual Result readHash(com::vmware::concord::thin_replica::Hash* hash);

  // Open a state subscription stream (connection has to be established before).
  // A state data stream will be open after openStateStream returns if and only
  // if openDataStream returns Result::kSuccess; a stream will not be open in
  // the failure and timeout cases.
  virtual Result openStateStream(const com::vmware::concord::thin_replica::ReadStateRequest& request);

  // Cancel and close a state subscription stream. This function is intended for
  // proactive cancellation of a (potentially) in-progress state stream that the
  // caller intends to stop and end, possibly before the streams intended
  // completion.
  virtual void cancelStateStream();

  // Properly complete and close a state subscription stream that has finished
  // streaming data. This function is intended for proper ending of state
  // subscription streams that the caller has exhausted the available state
  // updates from by the intended means. Returns false if the underlying
  // communication implementation reports an error while trying to properly end
  // the stream and true otherwise.
  virtual Result closeStateStream();

  virtual bool hasStateStream();

  // Read state data from an existing open state data stream. Note readState
  // will automatically close the open state stream in the event it has to time
  // out the read.
  virtual Result readState(com::vmware::concord::thin_replica::Data* data);

  // Read the hash of the state defined by the request (connection has to be
  // established before).
  virtual Result readStateHash(const com::vmware::concord::thin_replica::ReadStateHashRequest& request,
                               com::vmware::concord::thin_replica::Hash* hash);

  // Helper to print/log connection details
  friend std::ostream& operator<<(std::ostream& os, const TrsConnection& trsc) {
    return os << trsc.client_id_ << " (" << trsc.address_ << ")";
  }

 protected:
  // Helper function to deal with gRPC
  void createStub();
  void createChannel();

  log4cplus::Logger logger_;

  // Connection identifiers
  std::string address_;
  std::string client_id_;

  // Subscription streams
  std::unique_ptr<grpc::ClientContext> data_context_;
  std::unique_ptr<grpc::ClientReaderInterface<com::vmware::concord::thin_replica::Data>> data_stream_;
  std::unique_ptr<grpc::ClientContext> state_context_;
  std::unique_ptr<grpc::ClientReaderInterface<com::vmware::concord::thin_replica::Data>> state_stream_;
  std::unique_ptr<grpc::ClientContext> hash_context_;
  std::unique_ptr<grpc::ClientReaderInterface<com::vmware::concord::thin_replica::Hash>> hash_stream_;

  // gRPC connection
  std::shared_ptr<grpc::Channel> channel_;
  std::unique_ptr<com::vmware::concord::thin_replica::ThinReplica::StubInterface> stub_;

  // TRS connection config
  std::unique_ptr<TrsConnectionConfig> config_;

  std::chrono::milliseconds data_timeout_;
  std::chrono::milliseconds hash_timeout_;

  // This method reads certificates from file if TLS is enabled
  void readCert(const std::string& input_filename, std::string& out_data);

  // This method gets the client_id from the OU field in the client certificate
  std::string getClientIdFromClientCert(const std::string& client_cert_path);

  // This method is used by getClientIdFromClientCert to get the client_id from
  // the subject in the client certificate
  std::string parseClientIdFromSubject(const std::string& subject_str);
};

}  // namespace thin_replica_client

#endif  // THIN_REPLICA_CLIENT_TRS_CONNECTION_HPP_
