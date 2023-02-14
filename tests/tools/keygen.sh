#!/bin/sh

if [ "$CADIR" = "" ]; then
    CADIR=$TESTDIR
    if [ "$CADIR" = "" ]; then
        echo 'Need CA directory.' >&2
        exit 1
    fi
fi

if [ ! -f $CADIR ]; then
    mkdir -p $CADIR
fi

# If key files exist, skip key generation.
if [ -f server.key ] || ([ -n "$SKIPSSL" ] && [ "$SKIPSSL" != "0" ]) ; then
  exit 0
fi

echo $CN
if [ "$CN" = "" ]; then
    CN=*`hostname -f`
fi

# Setup ssl certificate
# Create root key
openssl genrsa -out $CADIR/root.key 4096
chmod 400 $CADIR/root.key
# Create and self sign the root certificate
openssl req -x509 -new -nodes -key $CADIR/root.key -days 30 -out $CADIR/root.crt \
            -subj "/C=US/ST=New York/L=New York/O=Bloomberg/OU=Comdb2/CN=$CN"

# Set up our CA
mkdir $CADIR/certs
mkdir $CADIR/crl
mkdir $CADIR/newcerts
mkdir $CADIR/private
touch $CADIR/index.txt
echo 01 >$CADIR/crlnumber

# Set up our CA
echo '
[ ca ]
default_ca = testsuite_ca
[ testsuite_ca ]
dir = '$CADIR'
certs = $dir/certs
crl_dir = $dir/crl
database = $dir/index.txt
new_certs_dir = $dir/newcerts
certificate = $dir/root.crt
serial = $dir/root.srl
crlnumber = $dir/crlnumber
crl = $dir/crl.pem
private_key = $dir/root.key
RANDFILE = $dir/private/.rand
x509_extensions = usr_cert
name_opt = ca_default
cert_opt = ca_default
default_days = 365
default_crl_days = 30
default_md = default
preserve = no
policy = policy_match
' > $CADIR/ca.cnf

if [ "$SCN" = "" ]; then
	SCN=$CN
fi

if [ "$CCN" = "" ]; then
	CCN=$CN
fi

ssubj="/C=US/ST=New York/L=New York/O=Bloomberg/OU=Comdb2/CN=$SCN/host=ssldbname*/UID=roborivers"
csubj="/C=US/ST=New York/L=New York/O=Bloomberg/OU=Comdb2/CN=$CCN/host=ssldbname*/UID=roborivers"

# Create server key
openssl genrsa -out $CADIR/server.key 4096
# Create signing request
openssl req -new -key $CADIR/server.key -out $CADIR/server.key.csr \
            -subj "$ssubj"
# Sign server key
openssl x509 -req -in $CADIR/server.key.csr -CA $CADIR/root.crt -CAkey $CADIR/root.key \
             -CAcreateserial -out $CADIR/server.crt -days 10
# Change key permissions
chmod 400 $CADIR/server.key

# Create client key
openssl genrsa -out $CADIR/client.key 4096
# Create signing request. The STREET attribute is for jdbc testing.
openssl req -new -key $CADIR/client.key -out $CADIR/client.key.csr \
            -subj "/street=jdbc*$csubj"
# Sign client key
openssl x509 -req -in $CADIR/client.key.csr -CA $CADIR/root.crt -CAkey $CADIR/root.key \
             -CAserial $CADIR/root.srl -out $CADIR/client.crt -days 10
# Change key permissions
chmod 400 $CADIR/client.key

# Create revoked key
openssl genrsa -out $CADIR/revoked.key 4096
# Create signing request
openssl req -new -key $CADIR/revoked.key -out $CADIR/revoked.key.csr \
            -subj "$csubj"
# Sign revoked key
openssl x509 -req -in $CADIR/revoked.key.csr -CA $CADIR/root.crt -CAkey $CADIR/root.key \
             -CAserial $CADIR/root.srl -out $CADIR/revoked.crt -days 10
# Change key permissions
chmod 400 $CADIR/revoked.key
## Revoke
openssl ca -config $CADIR/ca.cnf -revoke $CADIR/revoked.crt \
           -keyfile $CADIR/root.key -cert $CADIR/root.crt
# Generate CRL
openssl ca -config $CADIR/ca.cnf -gencrl -out $CADIR/root.crl

# SAN config
san="DNS:$(hostname), DNS: $(hostname -f)"
for ip in $(hostname -I); do
  san="$san, DNS: $ip"
done
for node in $CLUSTER; do
  san="$san, DNS:$(ssh -o StrictHostKeyChecking=no $node 'hostname -f')"
  for ip in $(ssh -o StrictHostKeyChecking=no $node 'hostname -I'); do
    san="$san, DNS: $ip"
  done
done

echo "
[req]
distinguished_name = req_distinguished_name
req_extensions = v3_req
x509_extensions = v3_ca
prompt = no

[req_distinguished_name]
C = US
ST = New York
L = New York
O = Bloomberg
OU = Comdb2
CN = *.bloomberg.com

[v3_req]
subjectAltName = $san

[v3_ca]
subjectAltName = DNS:*.bloomberg.com
" >$CADIR/san.cnf

# Create SAN key
openssl genrsa -out $CADIR/san.key 4096
# Create signing request
openssl req -new -key $CADIR/san.key -out $CADIR/san.key.csr \
            -subj "/C=US/ST=New York/L=New York/O=Bloomberg/OU=Comdb2/CN=www.example.com"
# Sign SAN key
openssl x509 -req -in $CADIR/san.key.csr -CA $CADIR/root.crt -CAkey $CADIR/root.key \
             -CAcreateserial -out $CADIR/san.crt -days 10 -extensions v3_req -extfile $CADIR/san.cnf
# Change key permissions
chmod 700 $CADIR/san.key
cp $CADIR/san.crt /tmp/san.crt
cp $CADIR/san.key /tmp/san.key

myhostname=`hostname`
# copy over SSL certificate and change permission on private key
for node in $CLUSTER; do
  if [ "$node" = "$myhostname" ] ; then
    continue
  fi
  ssh -o StrictHostKeyChecking=no $node "mkdir -p $CADIR"
  fqdn=`ssh -o StrictHostKeyChecking=no $node 'hostname -f'`

  # Create server_$fqdn key
  openssl genrsa -out $CADIR/server_$fqdn.key 4096
  # Create signing request
  openssl req -new -key $CADIR/server_$fqdn.key -out $CADIR/server_$fqdn.key.csr \
              -subj "/C=US/ST=New York/L=New York/O=Bloomberg/OU=Comdb2/CN=$fqdn/host=ssldbname*"
  # Sign server_$fqdn key
  openssl x509 -req -in $CADIR/server_$fqdn.key.csr -CA $CADIR/root.crt -CAkey $CADIR/root.key \
               -CAcreateserial -out $CADIR/server_$fqdn.crt -days 10 -extensions v3_req -extfile $CADIR/san.cnf
  # Change key permissions
  chmod 400 $CADIR/server_$fqdn.key

  scp -o StrictHostKeyChecking=no $CADIR/server_$fqdn.crt $node:$CADIR/server.crt
  scp -o StrictHostKeyChecking=no $CADIR/server_$fqdn.key $node:$CADIR/server.key
  scp -o StrictHostKeyChecking=no $CADIR/san.crt $node:/tmp/san.crt
  scp -o StrictHostKeyChecking=no $CADIR/san.key $node:/tmp/san.key
  scp -o StrictHostKeyChecking=no $CADIR/root.crt $CADIR/root.crl $node:$CADIR
done

# Generate client side CRL.
for crt in `find ${CADIR} -name 'server*.crt'`; do
  openssl ca -config ${CADIR}/ca.cnf -revoke $crt \
             -keyfile ${CADIR}/root.key -cert ${CADIR}/root.crt
done
openssl ca -config ${CADIR}/ca.cnf -gencrl -out ${CADIR}/client.crl
