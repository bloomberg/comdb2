---
title: Configuring Secure Sockets Layer (SSL)
keywords: Security SSL TLS Encryption
tags: [Security, SSL, TLS, Encryption]
sidebar: mydoc_sidebar
permalink: ssl.html
---

Comdb2 can be configured to secure network traffic with Transport Layer Security (TLS).
TLS, which is the successor to SSL, offers stronger encryption than SSL. TLS also provides
identity verification using X509 standard. Therefore Comdb2 uses TLS protocol only.

In order to configure SSL, OpenSSL run-times are required on both clients and servers.


## Basic SSL Configuration

The server needs an OpenSSL key pair in PEM format to start in SSL mode. By default,
the server searches `server.crt` and `server.key` under the data directory for
server certificate and key, respectively. Alternatively, You could specify the paths
to these files by adding the following directives in the LRL:

```
ssl_cert /path/to/certificate
ssl_key /path/to/key
```

For security reasons, the server will reject the key and fail to start if the key
can be read/written/executed by other users. You could use the following command to
make the key read only by the owner:

``` shell
chmod 400 <key>
```

If the key has a passphrase, the server will prompt for the passphrase and will not proceed
until the passphrase is entered.

Comdb2 accepts both SSL and plaintext connections when running in the default SSL mode.
To enforce SSL-only connections from clients, you would change SSL mode to `REQUIRE`
in the LRL:

```
ssl_client_mode REQUIRE
```

The client library `libcdb2api` automatically chooses SSL or plaintext
based on the server configuration: it uses SSL if SSL is required by the
server, and uses plaintext otherwise.


## Using Secure Connections to Prevent Network Attacks

SSL offers protection against two types of network attacks: Eavesdropping and Man-in-the-middle attacks.

### Protecting the Connections against Eavesdropping

With an unencrypted connection, a third party with access to the network can
examine the traffic and inspect the data. Such attacks are called eavesdropping.
To prevent eavesdropping, you would configure `libcdb2api` to always use SSL by
adding the following in `comdb2db.cfg` or in the database configuration file:

```
ssl_mode REQUIRE
```

If the server supports SSL, SSL connections will be established. Otherwise, `libcdb2api` will
not attempt to connect.

### Protecting the Connections against Man-in-the-middle Attacks

Even with an encrypted connection, a third party with access to the network can intercept,
modify and relay messages between the client and server and make them think that they are
talking to each other securely. Such attacks are called Man-in-the-middle (MITM) attacks
(also known as replay attacks or active eavesdropping). To prevent MITM, the client needs
to verify server identity. To do it, you would have a file which contains certificates of
the certificate authorities (CA) you trust, and add the following to `comdb2db.cfg` or
the database configuration file:

```
ssl_ca /path/to/ca/certificate
```

The client will disconnect if the server certificate is not signed by any CA in the trusted CA certificate file.

#### Authenticating the Client to the Server

The server can also be configured to verify client identities. By default, the server
searches `root.crt` under the data directory for the server CA certificate file. Alternatively
you could specify the path to the CA certificate by adding the following in the LRL:

```
ssl_ca /path/to/ca/certificates
```

On the client side, you would specify the paths to the client key pair by adding
the following to `comdb2db.cfg` or the database configuration file:

```
ssl_cert /path/to/certificate
ssl_key  /path/to/key
```

If a client certificate is present, it will be verified against the CA certificates.
The connection will be rejected if the server fails to verify the client certificate.

To always require client certificates, you would change SSL mode in the LRL:

```
ssl_client_mode VERIFY_CA
```

Now if a client certificate is not present, the connection will be rejected as well.

For maximum security, you could request host name validation by changing SSL mode the LRL:

```shell
ssl_client_mode VERIFY_HOSTNAME
```

If the client host name mismatches the DNS records embedded in the client certificate,
the connection will be rejected by the server as well.


### Protecting Intra-Cluster Communication

If the database cluster is in an uncontrolled environment where intra-cluster
network attacks may occur, you could encrypt the communication between
the master and replicants by changing replicant SSL mode and bouncing
on all nodes:

```shell
ssl_replicant_mode REQUIRE
```

To perform both intra-cluster encryption and identity verification,
you would change replicant SSL mode to `VERIFY_CA` and bounce on all nodes:

```shell
ssl_replicant_mode VERIFY_CA
```

The nodes will verify each other's certificate against their own trusted CA file. Nodes that fail the certificate verification are refused to join the cluster.

For maximum security, you could request host name validation by changing
the replicant SSL mode to `VERIFY_HOSTNAME` and bounce on all nodes.
Nodes whose host names mismatch the DNS records embedded in their certificates
are refused to join the cluster.


## Server SSL Configuration Summary

| LRL Directive | Description | Default Value |
|---------------|-------------|---------------|
| `ssl_client_mode mode` | Can be one of `ALLOW`, `REQUIRE`, `VERIFY_CA` and `VERIFY_HOSTNAME` | `ALLOW` |
| `ssl_replicant_mode mode` | Can be one of `ALLOW`, `REQUIRE`, `VERIFY_CA` and `VERIFY_HOSTNAME` | `ALLOW` |
| `ssl_cert_path path` | Directory containing the server certificate files. Comdb2 searches `server.crt`, `server.key` and `root.crt` for the server certificate, key and trusted CAs respectively | The data directory |
| `ssl_cert file`| Path to the certificate | `<ssl_cert_path>/server.crt` |
| `ssl_key file` | Path to the key | `<ssl_cert_path>/server.key` |
| `ssl_ca file` | Path to the trusted CA certificates | `<ssl_cert_path>/root.crt` |


## Client SSL Configuration Summary

| Option | Description | Default Value |
|---------------|-------------|--------|
| `ssl_mode mode` | Can be one of `ALLOW`, `REQUIRE`, `VERIFY_CA` and `VERIFY_HOSTNAME` | `ALLOW` |
| `ssl_cert_path path` | Directory containing client certificate files. libcdb2api searches `client.crt`, `client.key` and `root.crt` for the client certificate, key and trusted CAs respectively | `N/A` |
| `ssl_cert file` | Path to the client certificate | `<ssl_cert_path>/client.crt` |
| `ssl_key file` | Path to the client key | `<ssl_cert_path>/client.key` |
| `ssl_ca file` | Path to the trusted CA certificates. | `<ssl_cert_path>/root.crt` |
| `ssl_session_cache 1/0` | Enable SSL client-side session cache. | `0` |


## SSL Mode Summary

|  Mode  | Encryption  | MITM | Overhead  |
|---|---|---|---|
|  `ALLOW` | Maybe |  Yes |  SSL negotiation<sup>[1](#sslfootnote)</sup> + TLS protocol overhead if the server requires SSL. No overhead otherwise. |
|  `REQUIRE` | Yes  | Yes  |  SSL negotiation<sup>[1](#sslfootnote)</sup> + TLS protocol overhead |
| `VERIFY-CA` | Yes | Maybe if signed by 3rd party CA. No if self-signed or signed by a local CA. | SSL negotiation<sup>[1](#sslfootnote)</sup> + TLS protocol overhead + certificate verification |
| `VERIFY-HOSTNAME` | Yes | No | SSL negotiation<sup>[1](#sslfootnote)</sup> + TLS protocol overhead + certificate verification + host name validation |

<a name="sslfootnote">[1]</a>: In order to establish an SSL connection to server, the client needs to negotiate with the server over the plaintext connection before upgrading to SSL. This happens only once for each connection establishment.
