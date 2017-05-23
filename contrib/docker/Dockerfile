FROM ubuntu:xenial

RUN apt-get update && \
  apt-get install -y \
    liblz4-tool \
    libprotobuf-c1 \
    libreadline6 \
    libsqlite3-0 \
    libuuid1 \
    libz1 \
    libssl1.0.0 \
    libunwind8 \
    liblz4-1 \
    tzdata && \
  mkdir -p /tmp/cdb2 /var/cdb2 /etc/cdb2/config/conf.d && \
  rm -rf /var/lib/apt/lists/*

ENV PATH=${PATH}:/comdb2/bin

COPY ./build/bin /comdb2/bin

