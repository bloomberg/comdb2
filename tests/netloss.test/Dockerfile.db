FROM @image@

ENV PATH      $PATH:/opt/bb/bin

COPY testdb /opt/bb/var/cdb2/testdb
COPY init /init
COPY send /opt/bb/bin/send

ENTRYPOINT /init
