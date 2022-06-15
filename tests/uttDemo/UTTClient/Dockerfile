
# Image for running the UTT Demo Replica
FROM concordbft/utt-demo-runenv:0.39
LABEL Description="UTT Demo Client"

WORKDIR /concord/

# Copy the replica executable
COPY ./bin/utt_client ./

# Volumes
VOLUME [ "/concord/logs", "/concord/config" ]

# Start the client (the client id is passed by docker-compose)
ENTRYPOINT [ "./utt_client", "-n", "config/net_docker.txt" ]
