FROM ubuntu:xenial

RUN apt-get update && \
  apt-get install -y \
    bc \
    bison \
    build-essential \
    flex \
    gawk \
    liblz4-dev \
    libprotobuf-c-dev \
    libreadline-dev \
    libsqlite3-dev \
    libssl-dev \
    libunwind-dev \
    libz-dev \
    make \
    ncurses-dev \
    protobuf-c-compiler \
    tcl \
    uuid-dev \
    libz1 \
    liblz4-tool \
    libprotobuf-c1 \
    libsqlite3-0 \
    libuuid1 \
    libz1 \
    tzdata && \
  rm -rf /var/lib/apt/lists/*

COPY . /comdb2.build
RUN cd /comdb2.build && make clean

VOLUME /comdb2
