FROM ubuntu:xenial

ENV PATH      $PATH:/opt/bb/bin

RUN apt-get update && \
  apt-get install -y \
    liblz4-dev \
    make \
    libz1 \
    liblz4-tool \
    libprotobuf-c1 \
    libreadline-dev \
    libsqlite3-0 \
    libuuid1 \
    libssl1.0.0 \
    libz1 \
    libunwind8 \
    tzdata && \
  rm -rf /var/lib/apt/lists/*

COPY build /
