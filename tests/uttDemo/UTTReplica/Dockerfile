
# Image for running the UTT Demo Replica
FROM concordbft/utt-demo-runenv:0.39
LABEL Description="UTT Demo Replica"

WORKDIR /concord/

# Copy the replica executable
COPY ./bin/utt_replica ./

# Copy config
COPY ./config/logging.properties /concord/config/
COPY ./config/net_docker.txt /concord/config/
COPY ./config/replica_* /concord/config/
COPY ./config/utt_replica_* /concord/config/

# Volumes
VOLUME [ "/concord/logs", "/concord/rocksdb" ]

# Start the replica (the replica id is passed by docker-compose)
ENTRYPOINT [ "./utt_replica", "-k", "config/replica_", "-n", "config/net_docker.txt" ]
