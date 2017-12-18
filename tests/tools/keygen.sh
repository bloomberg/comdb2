#!/bin/sh
set -x 

if [ ! -f ${TESTDIR} ] ; then
    mkdir -p ${TESTDIR}
fi

# If key files exist, skip key generation.
if [ -f server.key ] || ([ -n "$SKIPSSL" ] && [ "$SKIPSSL" != "0" ]) ; then
  exit 0
fi

# Setup ssl certificate
openssl req -x509 -newkey rsa:4096 -keyout ${TESTDIR}/server.key \
    -out ${TESTDIR}/server.crt \
    -days 365 -nodes -subj \
    "/C=US/ST=New York/L=New York/O=Bloomberg/OU=Comdb2/CN=*.bloomberg.com"
cp ${TESTDIR}/server.crt ${TESTDIR}/root.crt
chmod 600 ${TESTDIR}/server.key
cp ${TESTDIR}/server.crt ${TESTDIR}/client.crt
cp ${TESTDIR}/server.key ${TESTDIR}/client.key
myhostname=`hostname`

# copy over SSL certificate and change permission on private key
for node in $CLUSTER; do
  if [ "$node" = "$myhostname" ] ; then
    continue
  fi
  ssh -o StrictHostKeyChecking=no $node "mkdir -p $TESTDIR"
  scp -o StrictHostKeyChecking=no ${TESTDIR}/server.crt ${TESTDIR}/server.key ${TESTDIR}/root.crt $node:${TESTDIR}
done
