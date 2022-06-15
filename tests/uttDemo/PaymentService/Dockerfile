
# Image for running the UTT Demo Replica
FROM concordbft/utt-demo-runenv:0.39
LABEL Description="UTT Demo Payment Service"

WORKDIR /concord/

# Copy the replica executable
COPY ./bin/payment_service ./

# Copy config
COPY ./config/logging.properties /concord/config/
COPY ./config/net_docker.txt /concord/config/

# Volumes
VOLUME [ "/concord/logs" ]

# Start the payment service (the id is passed by docker-compose)
ENTRYPOINT [ "./payment_service", "-f", "1", "-n", "config/net_docker.txt" ]
