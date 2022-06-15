
# Image for running the UTT Demo Replica
FROM concordbft/utt-demo-runenv:0.39
LABEL Description="UTT Demo Payment Service"

WORKDIR /concord/

# Copy the replica executable
COPY ./bin/payment_service ./

# Volumes
VOLUME [ "/concord/logs", "/concord/config" ]

# Start the payment service (the id is passed by docker-compose)
ENTRYPOINT [ "./payment_service", "-f", "1", "-n", "config/net_docker.txt" ]
