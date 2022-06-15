
# Image for running the UTT Demo Replica
FROM concordbft/utt-demo-runenv:0.39
LABEL Description="UTT Demo Client"

WORKDIR /concord/

# Copy the replica executable
COPY ./bin/utt_client ./

# Copy config
COPY ./config/logging.properties /concord/config/
COPY ./config/net_docker.txt /concord/config/
COPY ./config/utt_wallet_* /concord/config/

# Volumes
VOLUME [ "/concord/logs" ]

# Start the client (the client id is passed by docker-compose)
ENTRYPOINT [ "./utt_client", "-n", "config/net_docker.txt" ]
