FROM liquibase/liquibase:4.28.0

# Install PostgreSQL client
USER root
RUN apt-get update && \
    apt-get install -y postgresql-client && \
    rm -rf /var/lib/apt/lists/*

# Copy main script to the image
COPY ./run-setup.sh /run-setup.sh
RUN chmod +x /run-setup.sh

# Set working directory to /liquibase to match volume mount
WORKDIR /liquibase

# Switch back to liquibase user
USER liquibase

ENTRYPOINT ["/run-setup.sh"]
