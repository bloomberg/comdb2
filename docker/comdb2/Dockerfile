# Base image
FROM ubuntu:17.04

# Refresh the packages and create necessary directories
RUN apt-get -y update && \
    apt-get -y install openssl libprotobuf-c1 libunwind8 && \
    mkdir -p /opt/bb /var/lib/comdb2

# Labels
LABEL maintainer="opencomdb2@bloomberg.net" \
    version="1.0"

# Copy the Comdb2 binaries. Note: comdb2.tar.gz must be present
# in the current directory (i.e. the current upload context).
ADD ["comdb2.tar.gz", "/opt/bb"]

# Copy the entrypoint script
COPY docker-entrypoint.sh /usr/local/bin/

# Create a mount point for data directory
VOLUME /var/lib/comdb2

# Set the PATH
ENV PATH="/opt/bb/bin/:${PATH}"

# Expose pmux port
EXPOSE 5105

# Set the entrypoint
ENTRYPOINT ["docker-entrypoint.sh"]

# Set the CMD
CMD ["comdb2"]
