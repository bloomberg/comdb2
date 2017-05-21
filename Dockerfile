FROM debian:jessie

# Explicitly set user/group IDs
RUN groupadd -r comdb2 --gid=999 && useradd -r -g comdb2 --uid=999 comdb2

# Add gosu for easy step-down from root
ENV GOSU_VERSION 1.7
RUN set -x \
    && apt-get update && apt-get install -y --no-install-recommends ca-certificates wget && rm -rf /var/lib/apt/lists/* \
    && wget -O /usr/local/bin/gosu "https://github.com/tianon/gosu/releases/download/$GOSU_VERSION/gosu-$(dpkg --print-architecture)" \
    && wget -O /usr/local/bin/gosu.asc "https://github.com/tianon/gosu/releases/download/$GOSU_VERSION/gosu-$(dpkg --print-architecture).asc" \
    && export GNUPGHOME="$(mktemp -d)" \
    && gpg --keyserver ha.pool.sks-keyservers.net --recv-keys B42F6819007F00F88E364FD4036A9C25BF357DD4 \
    && gpg --batch --verify /usr/local/bin/gosu.asc /usr/local/bin/gosu \
    && rm -r "$GNUPGHOME" /usr/local/bin/gosu.asc \
    && chmod +x /usr/local/bin/gosu \
    && gosu nobody true \
    && apt-get purge -y --auto-remove ca-certificates wget

# Comdb2 dependencies
RUN apt-get update && \ 
    apt-get install -y \ 
    build-essential \
    bison flex libprotobuf-c-dev \
    libreadline-dev libsqlite3-dev \
    libssl-dev libunwind-dev libz1 \
    libz-dev make gawk protobuf-c-compiler \
    uuid-dev liblz4-tool libprotobuf-c1 \
    libreadline6 libsqlite3-0 libuuid1 \
    libz1 tzdata ncurses-dev tcl

# Build and install comdb2 server
ENV BUILD_DIR=/usr/src/comdb2
COPY . ${BUILD_DIR}
WORKDIR ${BUILD_DIR}

# Workaround till https://github.com/bloomberg/comdb2/issues/285 is fixed
RUN make berkdb/mem_berkdb.h 

RUN make && make install

ENV PATH      $PATH:/opt/bb/bin
ENV CDB2_DATA /opt/bb/tmp
ENV CDB2_DB   mydb

COPY docker/docker-entrypoint.sh /

ENTRYPOINT ["/docker-entrypoint.sh"]

EXPOSE 19000

CMD ["comdb2"]
