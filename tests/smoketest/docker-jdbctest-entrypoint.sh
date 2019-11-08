#!/usr/bin/env bash

set -e

export PATH=$PATH:/opt/bb/bin/

while [[ $1 = -* ]]; do
    # TODO: handle options, if any, here
    shift
done

if [[ $# -ne 3 ]]; then
    echo 'Expect database names.' >&2
    exit 1
fi

cadir=$1

# Generate Java key store and trust store
rm -f $cadir/client-keystore.pkcs12 $cadir/keystore $cadir/truststore
openssl pkcs12 -export \
        -in $cadir/client.crt \
        -inkey $cadir/client.key \
        -name cdb2clikey \
        -passout pass:cdb2jdbctest \
        -out $cadir/client-keystore.pkcs12
keytool -noprompt -importkeystore \
        -srckeystore $cadir/client-keystore.pkcs12 \
        -srcstoretype pkcs12 \
        -srcstorepass cdb2jdbctest \
        -destkeystore $cadir/keystore \
        -deststoretype JKS \
        -deststorepass cdb2jdbctest
keytool -noprompt -importcert \
        -alias cdb2ca \
        -file $cadir/root.crt \
        -keystore $cadir/truststore \
        -storepass cdb2jdbctest

chmod 644 $cadir/keystore $cadir/truststore

db=$2
ssldb=$3

pmux -l

host=$(hostname -i)

if [[ -f /opt/bb/etc/cdb2/config/comdb2.d/hostname.lrl ]]; then
    for dbname in /opt/bb/var/cdb2/*; do 
        dbname=${dbname##*/}
        comdb2 $dbname > /opt/bb/var/log/$dbname.out 2>&1 &
    done
else 
    echo "hostname $host" > /opt/bb/etc/cdb2/config/comdb2.d/hostname.lrl

    # Make a copy so we don't mess up permissions.
    cp -r $cadir /cert
    chown -R $(whoami) /cert

    comdb2 --create $db >/dev/null 2>&1 &
    comdb2 --create \
           --tunable 'ssl_client_mode VERIFY_DBNAME' \
           --tunable 'ssl_dbname_field street' \
           --tunable 'ssl_cert_path /cert' \
           $ssldb >/dev/null 2>&1 &
    wait

    for dbname in $*; do
        comdb2 $dbname > /opt/bb/var/log/$dbname.out 2>&1 &
    done
fi

wait
