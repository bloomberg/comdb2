FROM @image@

ENV PATH      $PATH:/opt/bb/bin

COPY init.client /init.client
COPY send /opt/bb/bin/send
COPY dotran ./dotran

ENTRYPOINT /init.client
