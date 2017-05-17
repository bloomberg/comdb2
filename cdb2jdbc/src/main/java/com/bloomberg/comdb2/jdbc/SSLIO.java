/* Copyright 2015 Bloomberg Finance L.P.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0
   
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License. */
package com.bloomberg.comdb2.jdbc;

import java.io.*;
import javax.naming.*;
import javax.naming.ldap.*;
import java.net.*;
import javax.net.ssl.*;
import java.util.*;
import java.util.logging.*;
import java.sql.*;
import java.security.*;
import java.security.cert.*;

import com.bloomberg.comdb2.jdbc.Constants.*;
import com.bloomberg.comdb2.jdbc.Sqlquery.*;

public class SSLIO extends SockIO {
    private SSL_MODE sslmode;

    /* Having a global SSLSocketFactory is nice however it can be
       problematic when the client uses different SSL certificates.
       So instead we create an SSLSocketFactory for every IO object
       and accept the overhead. */
    private SSLSocketFactory factory;

    private SSLSocketFactory createFactory(SSL_MODE mode,
            String cert, String certtype, String certpasswd,
            String ca, String catype, String capasswd)
        throws SSLException {

        KeyManagerFactory kf = null;
        TrustManagerFactory tf = null;

        try {
            kf = KeyManagerFactory.getInstance("PKIX");
            tf = TrustManagerFactory.getInstance("PKIX");
        } catch (NoSuchAlgorithmException nsae) {
            throw new SSLException("PKIX algorithm not found.", nsae);
        }

        /* Load user certificate. */
        if (cert != null && cert.length() > 0) {
            InputStream keyis = null;
            String kstype = "Unknown"; /* init to make javac happy */
            try {
                /* If no cert type specified, default to "jks" */
                if (certtype == null || certtype.length() == 0)
                    kstype = "jks";
                else
                    kstype = certtype;
                KeyStore clientks = KeyStore.getInstance(kstype);

                URL clientksurl = new URL(cert);
                keyis = clientksurl.openStream();

                /* init key with password */
                char[] passwd;
                if (certpasswd == null)
                    passwd = new char[0];
                else
                    passwd = certpasswd.toCharArray();

                clientks.load(keyis, passwd);
                kf.init(clientks, passwd);
            } catch (NoSuchAlgorithmException nsae) {
                throw new SSLException(kstype + " algorithm not found.", nsae);
            } catch (GeneralSecurityException gse) {
                throw new SSLException("Could not load client keystore.", gse);
            } catch (IOException ioe) {
                throw new SSLException("Could not open " + cert, ioe);
            } finally {
                if (keyis != null) {
                    try {
                        keyis.close();
                    } catch (IOException e) {/* silently ignore */}
                }
            }
        }

        /* Load user trusted CA cert. */
        if (ca != null && ca.length() > 0) {
            InputStream cais = null;
            String kstype = "Unknown"; /* init to make javac happy */
            try {
                /* If no ca type specified, default to "jks" */
                if (catype == null || catype.length() == 0)
                    kstype = "jks";
                else
                    kstype = catype;
                KeyStore caks = KeyStore.getInstance(kstype);

                URL caksurl = new URL(ca);
                cais = caksurl.openStream();

                /* init key with password */
                char[] passwd;
                if (capasswd == null)
                    passwd = new char[0];
                else
                    passwd = capasswd.toCharArray();

                caks.load(cais, passwd);
                tf.init(caks);
            } catch (NoSuchAlgorithmException nsae) {
                throw new SSLException(kstype + " algorithm not found.", nsae);
            } catch (GeneralSecurityException gse) {
                throw new SSLException("Could not load CA certificate.", gse);
            } catch (IOException ioe) {
                throw new SSLException("Could not open " + ca, ioe);
            } finally {
                if (cais != null) {
                    try {
                        cais.close();
                    } catch (IOException e) {/* silently ignore */}
                }
            }
        }

        /* create ssl context */
        SSLContext sslctx = null;
        try {
            sslctx = SSLContext.getInstance("TLS");
            KeyManager[] kms = null;
            if (cert != null && cert.length() > 0)
                kms = kf.getKeyManagers();

            TrustManager[] tms = null;
            if (ca != null && ca.length() > 0)
                tms = tf.getTrustManagers();
            else {
                tms = new X509TrustManager[] {
                    new X509TrustManager() {
                        public void checkClientTrusted(
                                X509Certificate[] chain, String authType) {
                            /* do nothing */
                        }
                        public void checkServerTrusted(
                                X509Certificate[] chain, String authType)
                            throws CertificateException {
                                /* do nothing */
                            }
                        public X509Certificate[] getAcceptedIssuers() {
                            return null;
                        }
                    }
                };
            }

            sslctx.init(kms, tms, null);
            return sslctx.getSocketFactory();
        } catch (NoSuchAlgorithmException nsae) {
            throw new SSLException("TLS protocol not found.", nsae);
        } catch (GeneralSecurityException gse) {
            throw new SSLException("Could not initialize SSL context.", gse);
        }
    }

    /* 0: SSL; 1: plaintext; -1: socket broken. */
    private int negotiate() {
        NewSqlHeader nsh = new NewSqlHeader(CDB2RequestType.SSLCONN_VALUE,
                0, 0, 0);
        try {
            write(nsh.toBytes());
            flush();

            byte[] code = new byte[1];
            if (read(code) != 1)
                return -1;

            if (code[0] != 'Y')
                return 1;

            return 0;
        } catch (IOException ioe) {
            return -1;
        }
    }

    private static boolean matchHost(String cert, String host) {
        /* return true if exact match */
        if (cert.equalsIgnoreCase(host))
            return true;

        if (cert.startsWith("*")) {
            int dot = host.indexOf('.');
            if (dot < 0)
                return false;
            return host.substring(dot).equalsIgnoreCase(cert.substring(1));
        }

        return false;
    }

    private boolean verify() {
        if (sslmode != SSL_MODE.VERIFY_CA
                && sslmode != SSL_MODE.VERIFY_HOSTNAME)
            return true;

        SSLSession sess = ((SSLSocket)sock).getSession();
        java.security.cert.Certificate[] servCerts;

        try {
            servCerts = sess.getPeerCertificates();
        } catch (SSLPeerUnverifiedException pue) {
            return false;
        }

        if (servCerts == null || servCerts.length == 0)
            return false;

        if (sslmode == SSL_MODE.VERIFY_CA)
            return true;

        X509Certificate servCert = (X509Certificate)servCerts[0];
        String peerHost = sock.getInetAddress().getCanonicalHostName();

        /* Match SANs */
        try {
            Collection<List<?>> sans = servCert.getSubjectAlternativeNames();
            if (sans != null) {
                for (List san : sans) {
                    /* 2 is DNS name */
                    if ((Integer)(san.get(0)) == 2) {
                        String dns = (String)(san.get(1));
                        if (matchHost(dns, peerHost))
                            return true;
                    }
                }
                /* RFC 6125 */
                return false;
            }
        } catch (CertificateParsingException cpe) {
            /* malformed certificate, return false */
            return false;
        }

        /* Match CN */
        LdapName DN;
        try {
            DN = new LdapName(servCert.getSubjectX500Principal().getName());
        } catch (InvalidNameException ine) {
            return false;
        }

        String CN = null;
        for (Rdn rdn : DN.getRdns()) {
            if ("CN".equals(rdn.getType())) {
                CN = (String)rdn.getValue();
                break;
            }
        }

        if (CN == null)
            return false;
        if (matchHost(CN, peerHost))
            return true;
        return false;
    }

    /* If an IOException is thrown by the constructor, 
       callers should not attempt to retry SSL. */
    public SSLIO(SockIO io, SSL_MODE mode,
            String cert, String certtype, String certpasswd,
            String ca, String catype, String capasswd) throws SSLException {
        if ((mode == SSL_MODE.VERIFY_CA || mode == SSL_MODE.VERIFY_HOSTNAME)
                && (ca == null || ca.length() == 0))
            throw new SSLException("Trust store required "
                    + "for server verification.");
        /* We require that the socket must be open first such that we know
           any IOException below is an unrecoverable SSL layer error. */
        if (!io.opened)
            throw new SSLException("The underlying socket must be "
                    + "connected first.");

        /* Copy over all attributes of the SockIO object */
        sock = io.sock;
        out = io.out;
        in = io.in;
        host = io.host;
        port = io.port;
        tcpbufsz = io.tcpbufsz;
        opened = true;
        sslmode = mode;

        /* Negotiate */
        int rc = negotiate();
        if (rc < 0) /* socket broken */
            throw new SSLException("Broken pipe");
        if (rc > 0) {
            if (sslmode == SSL_MODE.ALLOW)
                return;
            throw new SSLHandshakeException("Server does not support SSL.");
        }

        /* Context */
        factory = createFactory(mode, cert, certtype, certpasswd,
                                ca, catype, capasswd);

        /* Connect */
        SSLSocket sslsock;
        try {
            sslsock = (SSLSocket)factory.createSocket(sock, host, port, true);
            /* Manually initiate handshake so we can get
               any protocol exceptions as soon as possible. */
            sslsock.startHandshake();
        } catch (IOException ioe) {
            throw new SSLException("Could not start SSL handshake.", ioe);
        }

        /* Verify */
        if (!verify()) {
            try {
                close();
            } catch (IOException e) {/* ignore */}

            throw new SSLException("Could not verify server identity.");
        }

        sock = sslsock;
        try {
            out = new BufferedOutputStream(sock.getOutputStream());
            in = new BufferedInputStream(sock.getInputStream());
        } catch (IOException ioe) {
            throw new SSLException("Could not get stream from socket", ioe);
        }
    }

    /* We can reach here only if the 1st SSL connection attempt
       performed by the constructor succeeded. So callers should
       retry if the function returns false. */
    @Override
    public boolean open() {
        /* Open a plaintext connection first. */
        if (!super.open())
            return false;

        /* Negotiate over plaintext */
        int rc = negotiate();
        if (rc < 0) /* socket broken */
            return false;
        if (rc > 0) /* plaintext server */
            return (sslmode == SSL_MODE.ALLOW);

        SSLSocket sslsock;
        try {
            sslsock = (SSLSocket)factory.createSocket(sock, host, port, true);
            sslsock.startHandshake();
        } catch (IOException ie) {
            try {
                super.close();
            } catch (IOException ioe) {/* ignore */}
            return false;
        }

        /* Verify */
        if (!verify()) {
            try {
                close();
            } catch (IOException e) {/* ignore */}
            try {
                super.close();
            } catch (IOException ioe) {/* ignore */}
            return false;
        }

        sock = sslsock;
        try {
            out = new BufferedOutputStream(sock.getOutputStream());
            in = new BufferedInputStream(sock.getInputStream());
            return true;
        } catch (IOException ioe) {
            return false;
        }
    }
}
