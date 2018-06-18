FROM centos:latest

# Comdb2 uses libcrypto. Therefore OpenSSL is still needed.
RUN \
   yum install -y epel-release && \
   yum install -y cmake3 gcc gcc-c++ protobuf-c libunwind libunwind-devel   \
   protobuf-c-devel byacc flex openssl openssl-devel openssl-libs         \
   readline-devel sqlite sqlite-devel libuuid libuuid-devel zlib-devel    \
   zlib lz4-devel gawk tcl lz4 rpm-build which

EXPOSE 5105

COPY . /comdb2/

WORKDIR /comdb2/smoketestbuild/

RUN cmake3 .. -DWITH_SSL=0 && make && make install

ENV PATH="/opt/bb/bin:${PATH}"
