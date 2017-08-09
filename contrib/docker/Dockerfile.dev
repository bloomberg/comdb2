FROM comdb2-standalone:7.0.0pre

ENV PATH      $PATH:/opt/bb/bin

RUN apt-get update && apt-get install -y vim gdb iputils-ping strace

EXPOSE 5105

COPY contrib/docker/docker-dev-entrypoint.sh /

ENTRYPOINT ["/docker-dev-entrypoint.sh"]
