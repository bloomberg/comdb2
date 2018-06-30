FROM ubuntu:latest

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && \
  apt-get install -y \
    cmake \
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
    file \
    tzdata && \
  ln -fs /usr/share/zoneinfo/America/New_York /etc/localtime && \
  dpkg-reconfigure --frontend noninteractive tzdata && \
  rm -rf /var/lib/apt/lists/*

EXPOSE 5105

COPY . /comdb2/

WORKDIR /comdb2/smoketestbuild

RUN cmake .. && make package && dpkg -i comdb2*.deb

ENV PATH="/opt/bb/bin:${PATH}"
