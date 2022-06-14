
# Image for running the UTT Demo Replica
# FROM concordbft/concord-bft:0.39
FROM utt-demo-run:latest
LABEL Description="UTT Demo Payment Service"

WORKDIR /concord/

# Copy the replica executable
COPY ./bin/payment_service ./

# Volumes
VOLUME [ "/concord/logs", "/concord/config" ]

# Start the payment service (the id is passed by docker-compose)
ENTRYPOINT [ "./payment_service", "-f", "1", "-n", "config/net_docker.txt" ]
