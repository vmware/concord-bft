
# Image for running the UTT Demo Replica
FROM concordbft/concord-bft:0.39
LABEL Description="UTT Demo Replica"

WORKDIR /concord/

# Copy the replica executable
COPY ./bin/utt_replica ./

# Volumes
VOLUME [ "/concord/logs", "/concord/config", "/concord/rocksdb" ]

# Start the replica (the replica id is passed by docker-compose)
ENTRYPOINT [ "./utt_replica", "-k", "config/replica_", "-n", "config/net_docker.txt" ]
