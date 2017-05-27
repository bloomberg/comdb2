#!/bin/sh
set -e
openssl req -x509 -newkey rsa:4096 -keyout $1/badkey.pem -out $1/badcrt.pem \
    -days 365 -nodes -subj \
    "/C=US/ST=New York/L=New York/O=Bloomberg/OU=Comdb2/CN=*.bloomberg.com"
chmod a+r $1/badkey.pem $1/badcrt.pem
cp $1/badcrt.pem $1/badca.pem
exit 0
