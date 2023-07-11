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
import java.util.*;
import java.util.logging.*;
import javax.net.ssl.*;
import java.text.MessageFormat;

import com.bloomberg.comdb2.jdbc.Cdb2Query.*;
import com.bloomberg.comdb2.jdbc.Constants.*;
import com.bloomberg.comdb2.jdbc.Sqlquery.*;
import com.bloomberg.comdb2.jdbc.Sqlresponse.*;

/**
 * A Java implementation of Mohit's C API.
 * 
 * @author Rivers Zhang
 * @author Sebastien Blind
 */
public class Comdb2Handle extends AbstractConnection {
    private static Logger logger = Logger.getLogger(Comdb2Handle.class.getName());

    static int POLICY_RANDOMROOM = 1;
    static int POLICY_RANDOM = 2;
    static int POLICY_ROOM = 3;
    int myPolicy = POLICY_RANDOMROOM;
    boolean isDirectCpu;

    ArrayList<String> comdb2dbHosts = new ArrayList<String>();
    String comdb2dbName;
    int comdb2dbDbNum;

    String myDbName;
    int myDbNum;
    String myDbCluster;
    ArrayList<String> myDbHosts = new ArrayList<String>();
    ArrayList<Integer> myDbPorts = new ArrayList<Integer>();
    int overriddenPort = -1;
    int prefIdx = -1;
    int dbHostIdx = -1;
    int dbHostConnected = -1;
    int masterIndexInMyDbHosts = -1;

    String prefmach;
    String defaultType;
    String machineRoom;
    boolean hasUserPort;
    int portMuxPort = 5105;
    String dnssuffix = "bdns.bloomberg.com";
    int numHostsSameRoom;
    boolean hasUserTcpSz;
    int tcpbufsz;
    int age = 180; /* default max age 180 seconds */
    boolean pmuxrte = false;
    boolean verifyretry = true;
    boolean stmteffects = true;
    int soTimeout = 5000;
    boolean hasComdb2dbTimeout;
    int comdb2dbTimeout = 5000;
    boolean hasConnectTimeout;
    int connectTimeout = 100;
    int dbinfoTimeout = 500;

    private boolean in_retry = false;
    private boolean temp_trans = false;
    private boolean debug = false;
    private Cdb2SqlResponse firstResp;
    private Cdb2SqlResponse lastResp;

    private Object lock = new Object();
    private volatile boolean opened = false;

    /* flag to indicate if the handle is in a transaction */
    private boolean inTxn = false;
    private String driverErrStr = null;

    HashMap<String, Cdb2BindValue> bindVars;
    HashMap<Integer, Cdb2BindValue> bindVarsByIndex;
    private List<String> sentSetStmts; // Collection of "set" SQL statements sent for this Connection
    private List<String> pendingSetStmts; // Collection of "set" SQL statements pending to be sent

    private boolean ack = false;
    private boolean skipDrain = false;
    private boolean clearAckOnClose = true;

    private boolean isRead;
    private String lastSql;
    private long rowsRead;
    private String uuid;
    private String stringCnonce;
    private byte[] cnonce;
    private int maxretries = 20;
    private int minretries = 3; /* how many times a non-hasql statement can retry */
    private boolean retryAll = false;
    private int snapshotFile;
    private int snapshotOffset;
    private boolean isHASql = false;
    private int isRetry;
    private int errorInTxn = 0;
    private boolean readIntransResults = true;
    private boolean firstRecordRead = false;
    private long timestampus;

    /* The last Throwable. */
    private Throwable last_non_logical_err;

    /* no attempt to retry if the flag is on */
    private boolean sslerr = false;

    /* SSL support */
    private SSL_MODE sslmode = SSL_MODE.ALLOW;
    private String sslNIDDbName = "OU";
    private String sslcert, sslcertpass, sslcerttype;
    private String sslca, sslcapass, sslcatype;
    private String sslcrl;
    PEER_SSL_MODE peersslmode = PEER_SSL_MODE.PEER_SSL_ALLOW;

    /* argv0 */
    boolean sentClientInfo;
    boolean hasSendStack;
    boolean sendStack = true;

    boolean isBeforeFirst = true;
    boolean isFirst = false;
    boolean isAfterLast = false;

    static class QueryItem {
        byte[] buffer;
        boolean isRead;
        String sql;
        QueryItem(byte[] buffer, String sql, boolean isRead) {
            this.buffer = buffer;
            this.isRead = isRead;
            this.sql = sql;
        }
    }

    private List<QueryItem> queryList;

    public Comdb2Handle duplicate() {
        /* new an object. */
        Comdb2Handle ret = new Comdb2Handle(myDbName, myDbCluster);
        /* copy attributes over. */
        ret.myPolicy = myPolicy;
        ret.comdb2dbHosts.addAll(comdb2dbHosts);
        ret.comdb2dbName = comdb2dbName;
        ret.comdb2dbDbNum = comdb2dbDbNum;
        ret.myDbNum = myDbNum;
        ret.myDbHosts.addAll(myDbHosts);
        ret.myDbPorts.addAll(myDbPorts);
        ret.overriddenPort = overriddenPort;
        ret.defaultType = defaultType;
        ret.machineRoom = machineRoom;
        ret.hasUserPort = hasUserPort;
        ret.portMuxPort = portMuxPort;
        ret.dnssuffix = dnssuffix;
        ret.hasUserTcpSz = hasUserTcpSz;
        ret.tcpbufsz = tcpbufsz;
        ret.age = age;
        ret.pmuxrte = pmuxrte;
        ret.verifyretry = verifyretry;
        ret.soTimeout = soTimeout;
        ret.hasComdb2dbTimeout = hasComdb2dbTimeout;
        ret.comdb2dbTimeout = comdb2dbTimeout;
        ret.hasConnectTimeout = hasConnectTimeout;
        ret.connectTimeout = connectTimeout;
        ret.dbinfoTimeout = dbinfoTimeout;

        ret.sslmode = sslmode;
        ret.sslNIDDbName = sslNIDDbName;
        ret.sslcert = sslcert;
        ret.sslcertpass = sslcertpass;
        ret.sslcerttype = sslcerttype;
        ret.sslca = sslca;
        ret.sslcapass = sslcapass;
        ret.sslcatype = sslcatype;
        ret.peersslmode = peersslmode;

        ret.sentClientInfo = sentClientInfo;
        ret.hasSendStack = hasSendStack;
        ret.sendStack = sendStack;

        return ret;
    }

    /* Default constructor does not discover the database.
       This allows us to alter attributes of the handle
       without discovering twice. */
    public Comdb2Handle() {
        super(new ProtobufProtocol(), null);
        sentSetStmts = new LinkedList<String>();
        pendingSetStmts = new LinkedList<String>();

        /* CDB2JDBC_STATEMENT_QUERYEFFECTS and CDB2JDBC_VERIFY_RETRY
           are used by the Jepsen tests to change the driver's behaviors. */

        /* default                                  -> enable
         * export CDB2JDBC_STATEMENT_QUERYEFFECTS   -> enable
         * export CDB2JDBC_STATEMENT_QUERYEFFECTS=1 -> enable
         * export CDB2JDBC_STATEMENT_QUERYEFFECTS=0 -> disable
         */
        String queryeffectsEnv = System.getenv("CDB2JDBC_STATEMENT_QUERYEFFECTS");
        setStatementQueryEffects((queryeffectsEnv == null || !queryeffectsEnv.equals("0")));

        /*
         * default                        -> enable
         * export CDB2JDBC_VERIFY_RETRY   -> enable
         * export CDB2JDBC_VERIFY_RETRY=1 -> enable
         * export CDB2JDBC_VERIFY_RETRY=0 -> disable
         */
        String verifyRetryEnv = System.getenv("CDB2JDBC_VERIFY_RETRY");
        setVerifyRetry((verifyRetryEnv == null || !verifyRetryEnv.equals("0")));

        String userEnv = System.getenv("COMDB2_USER");
        if (userEnv != null) {
            addSetStatement("set user " + userEnv);
        }

        String passwordEnv = System.getenv("COMDB2_PASSWORD");
        if (passwordEnv != null) {
            addSetStatement("set password " + passwordEnv);
        }

        uuid = UUID.randomUUID().toString();
        tdlog(Level.FINEST, "Created handle with uuid %s", uuid);
        bindVars = new HashMap<String, Cdb2BindValue>();
        bindVarsByIndex = new HashMap<Integer, Cdb2BindValue>();
        queryList = new ArrayList<QueryItem>();
    }

    public Comdb2Handle(String dbname, String cluster) {
        this();
        myDbName = dbname;
        myDbCluster = cluster;
        try {
            lookup();
        }
        catch(NoDbHostFoundException e) {}
    }

    public void lookup() throws NoDbHostFoundException {
        DatabaseDiscovery.getDbHosts(this, false);
    }

    /* attribute setters - bb precious */
    public void setSSLMode(String mode) {
        if ("PREFER".equalsIgnoreCase(mode))
            sslmode = SSL_MODE.PREFER;
        if ("PREFER_VERIFY_CA".equalsIgnoreCase(mode))
            sslmode = SSL_MODE.PREFER_VERIFY_CA;
        if ("PREFER_VERIFY_HOSTNAME".equalsIgnoreCase(mode))
            sslmode = SSL_MODE.PREFER_VERIFY_HOSTNAME;
        if ("REQUIRE".equalsIgnoreCase(mode))
            sslmode = SSL_MODE.REQUIRE;
        else if ("VERIFY_CA".equalsIgnoreCase(mode))
            sslmode = SSL_MODE.VERIFY_CA;
        else if ("VERIFY_HOSTNAME".equalsIgnoreCase(mode))
            sslmode = SSL_MODE.VERIFY_HOSTNAME;
        else if (mode.toUpperCase().startsWith("VERIFY_DBNAME")) {
            sslmode = SSL_MODE.VERIFY_DBNAME;
            String[] splits = mode.split(",;\\s*");
            if (splits.length > 1)
                sslNIDDbName = splits[1].toUpperCase();
        } else if (mode.toUpperCase().startsWith("PREFER_VERIFY_DBNAME")) {
            sslmode = SSL_MODE.PREFER_VERIFY_DBNAME;
            String[] splits = mode.split(",;\\s*");
            if (splits.length > 1)
                sslNIDDbName = splits[1].toUpperCase();
        } else {
            sslmode = SSL_MODE.ALLOW;
        }
    }

    public void setSSLCrt(String crt) {
        sslcert = crt;
    }

    public void setSSLCrtPass(String crtpass) {
        sslcertpass = crtpass;
    }

    public void setSSLCrtType(String crttype) {
        sslcerttype = crttype;
    }

    public void setSSLCA(String ca) {
        sslca = ca;
    }

    public void setSSLCAPass(String capass) {
        sslcapass = capass;
    }

    public void setSSLCAType(String catype) {
        sslcatype = catype;
    }

    public void setSSLCRL(String crl) {
        sslcrl = crl;
    }

    public void setPrefMach(String mach) {
        prefmach = mach;
    }

    public void setOverriddenPort(int port) {
        overriddenPort = port;
    }

    public void setAllowPmuxRoute(boolean val) {
        pmuxrte = val;
        if (val)
            overriddenPort = portMuxPort;
    }

    public void setVerifyRetry(boolean val) {
        if (val == verifyretry)
            return;

        if (val) {
            removeSetStatement("set verifyretry off");
            addSetStatement("set verifyretry on");
        } else {
            removeSetStatement("set verifyretry on");
            addSetStatement("set verifyretry off");
        }

        verifyretry = val;
    }

    public void setSendStack(boolean val) {
        sendStack = val;
    }

    void addHosts(List<String> hosts) {
        myDbHosts.addAll(hosts);
    }

    void addPorts(List<Integer> ports) {
        myDbPorts.addAll(ports);
    }

    public void setDefaultType(String type) {
        defaultType = type;
    }

    public void setMachineRoom(String room) {
        machineRoom = room;
    }

    public void setPortMuxPort(int port) {
        portMuxPort = port;
        hasUserPort = true;
    }

    public void setDnsSuffix(String suffix) {
        dnssuffix = suffix;
    }

    public void setComdb2dbName(String name) {
        comdb2dbName = name;
    }

    public void setTcpBufSize(int sz) {
        tcpbufsz = sz;
        hasUserTcpSz = true;
    }

    public void setStatementQueryEffects(boolean val) {
        if (val == stmteffects)
            return;

        if (val) {
            removeSetStatement("set queryeffects transaction");
            addSetStatement("set queryeffects statement");
        } else {
            removeSetStatement("set queryeffects statement");
            addSetStatement("set queryeffects transaction");
        }

        stmteffects = val;
    }

    public void setSkipResultSetDrain(boolean val){
        this.skipDrain = val;
    }

    public void setClearAck(boolean val){
        this.clearAckOnClose = val;
    }

    public ArrayList<String> getDbHosts() throws NoDbHostFoundException{
        if (this.myDbHosts.size() == 0) {
            this.lookup();
        }
        tdlog(Level.FINEST, "comdb2Handle:this.myDbHosts is %s", this.myDbHosts);
        ArrayList<String> hosts = new ArrayList(this.myDbHosts.size());
        for (String item : this.myDbHosts) hosts.add(new String(item));
        return hosts;
    }

    public ArrayList<Integer> getDbPorts() throws NoDbHostFoundException {
        if (this.myDbPorts.size() == 0) {
            this.lookup();
        }
        tdlog(Level.FINEST, "comdb2Handle:this.myDbPorts is " + this.myDbPorts);
        ArrayList<Integer> ports = new ArrayList(myDbPorts.size());
        for (Integer item : this.myDbPorts) ports.add(new Integer(item));
        return ports;
    }

    public void setDebug(boolean on) {
        debug = on;
        DatabaseDiscovery.debug = on;
    }

    public void setMaxRetries(int retries) {
        maxretries = retries;
    }

    public int maxRetries() {
        return maxretries;
    }

    // Add td info to the beginning of the string
    private void tdlog(Level level, String str, Object... params) {
        /* Fast return if the level is not loggable. */
        if (!logger.isLoggable(level) && !debug)
            return;

        Level curlevel = logger.getLevel();
        String mach = "(not-connected)";
        if (dbHostConnected >= 0) {
            mach = myDbHosts.get(dbHostConnected);
        }

        String message = String.format(str, params);
        Object[] messageParams = new Object[] {
            Thread.currentThread().getId(),
                mach,
                snapshotFile,
                snapshotOffset,
                stringCnonce,
                message
        };
        logger.log(level,
                "td={0} mach={1} snapshotFile={2} snapshotOffset={3} cnonce={4}: {5}",
                messageParams);

        if (debug) {
            MessageFormat form = new MessageFormat("td={0} mach={1} snapshotFile={2} snapshotOffset={3} cnonce={4}: {5}");
            System.err.println(form.format(messageParams));
        }
    }

    public void setPolicy(String policy) {
        if ("random".equalsIgnoreCase(policy))
            myPolicy = POLICY_RANDOM;
        else if ("randomroom".equalsIgnoreCase(policy))
            myPolicy = POLICY_RANDOMROOM;
        else if ("room".equalsIgnoreCase(policy))
            myPolicy = POLICY_ROOM;
    }

    public void setComdb2dbMaxAge(int age) {
        this.age = age;
    }

    public void setDatabase(String db) {
        myDbName = db;
    }

    public void setCluster(String cluster) {
        myDbCluster = cluster;
    }

    private int retryQueries(int nretry, boolean runlast) {
        int rc;
        in_retry = true;
        rc = retryQueriesint(nretry, runlast);
        in_retry = false;
        return rc;
    }

    private int retryQueriesint(int nretry, boolean runlast) {

        int rc;

        if (!inTxn) {
            tdlog(Level.FINEST, "retryQueries returning immediately because inTxn is false");
            return 0;
        }

        if (!retryAll) {
            tdlog(Level.FINEST, "retryQueries returning immediately because retryAll is false");
            return 0;
        }

        if (snapshotFile <= 0 && queryList.size() > 0) { /* no snapshot info thus can't resume */
            /* this is a logical error. */
            last_non_logical_err = null;
            driverErrStr = "Database disconnected while in transaction.";
            return Errors.CDB2ERR_TRAN_IO_ERROR;
        }
        
        tdlog(Level.FINEST,
              "retryQueries: nretry=%d runlast=%b queryList.size()=%d",
              nretry, runlast, queryList.size());

        clearResp();
        isRetry = nretry;
        readIntransResults = true;
        inTxn = false;

        // Either we have a snapshot or the querylist is 0: send a begin
        if (!sendQuery("begin", null, true, 0, nretry, false)) {
            closeNoException();
            inTxn = true;
            return 1;
        }

        inTxn = true; /* begin sent. I am now in a transaction. */

        NewSqlHeader nsh;
        byte[] raw;
        if ((nsh = readNsh()) == null || (raw = readRaw(nsh.length)) == null) {
            closeNoException();
            return 1;
        }

        if (nsh.type == Sqlresponse.ResponseHeader.DBINFO_RESPONSE_VALUE) {
            try {
                Cdb2DbInfoResponse dbinfo = protocol.unpackDbInfoResp(raw);
                ArrayList<String> validHosts = new ArrayList<String>();
                ArrayList<Integer> validPorts = new ArrayList<Integer>();
                try {
                    DatabaseDiscovery.dbInfoQuery(this,
                            dbinfo, myDbName, myDbNum,
                            null, 0, validHosts, validPorts);
                } catch (IOException e) {
                    validHosts.clear();
                }

                if (validHosts.size() > 0) {
                    /* purge old hosts. add the new ones. */
                    myDbHosts.clear();
                    myDbHosts.addAll(validHosts);
                    myDbPorts.clear();
                    myDbPorts.addAll(validPorts);
                }
                closeNoException();
            } catch (IOException ioe) {
                last_non_logical_err = ioe;
            }
            return 1;
        }

        try {
            firstResp = protocol.unpackSqlResp(raw);
        } catch (IOException ioe) {
            driverErrStr = "Can't read response from db";
            last_non_logical_err = ioe;
            closeNoException();
            return 1;
        }

        while ((rc = next_int()) == Errors.CDB2_OK)
            ;

        // Above loop could drop connection
        if (dbHostConnected < 0) {
            driverErrStr = "Can't read next response from db";
            closeNoException();
            return 1;
        }

        for (int ii = 0, 
             len = runlast ? queryList.size() : queryList.size() - 1;
             ii < len; ++ii) {
            QueryItem item = queryList.get(ii);
            byte[] buffer = (byte[])item.buffer;
            nsh = new NewSqlHeader(RequestType.CDB2_REQUEST_CDB2QUERY,
                                                0, 0, buffer.length);

            tdlog(Level.FINEST, "retryQueries resending '%s'", item.sql);

            try {
                io.write(nsh.toBytes());
                io.write(buffer);
                io.flush();
            } catch (IOException e) {
                last_non_logical_err = e;
                driverErrStr = "Can't read response from db";
                tdlog(Level.FINEST, "Error resending '%s'", item.sql);
                closeNoException();
                return 1;
            }

            clearResp();

            if (!readIntransResults && !item.isRead) {
                tdlog(Level.FINEST,
                      "retryQueries continuing because readIntransResults is %b and item.isRead is %b",
                      readIntransResults, item.isRead);
                continue;
            }

            firstResp = readRecord();
            if (firstResp == null) {
                driverErrStr = "Can't read response from db";
                tdlog(Level.FINEST, "Error reading response on retry");
                closeNoException();
                return 1;
            }
            while ((rc = next_int()) == Errors.CDB2_OK);

            if (dbHostConnected < 0) {
                driverErrStr = "Can't read next response from db";
                closeNoException();
                return 1;
            }
        }
        clearResp();
        return 0;
    }

    private boolean retryQueriesAndSkip(int nretry, long skipNRows) {
        if (snapshotFile <= 0) {
            tdlog(Level.FINEST,
                  "retryQueriesAndSkip returning false immediately because snapshotFile is %d",
                  snapshotFile);
            return false;
        }

        retryAll = true;

        if (inTxn && retryQueries(nretry, false) != 0) {
            tdlog(Level.FINEST, "retryQueriesAndSkip retried queries and failed");
            return false;
        }

        isRetry = nretry;

        if (!sendQuery(lastSql, null, false, skipNRows, nretry, false))
            return false;

        firstResp = readRecord();
        boolean rcode = (firstResp == null) ? false : true;
        tdlog(Level.FINEST,
              "retryQueriesAndSkip: firstResp is %s returning %d",
              firstResp, rcode);
        return rcode;
    }

    private boolean sendQuery(String sql, List<Integer> types,
            boolean isBegin, long skipNRows, int nretry, boolean doAppend) {
        Cdb2Query query = new Cdb2Query();
        Cdb2SqlQuery sqlQuery = new Cdb2SqlQuery();
        query.cdb2SqlQuery = sqlQuery;

        if (!sentClientInfo) {
            sqlQuery.cinfo = new Cdb2ClientInfo();
            sqlQuery.cinfo.argv0 = Comdb2ClientInfo.getCallerClass();
            sqlQuery.cinfo.stack = Comdb2ClientInfo.getCallStack(32);
            sentClientInfo = true;
        }

        sqlQuery.dbName = myDbName;
        sqlQuery.sqlQuery = sql;

        sqlQuery.bindVars.addAll(bindVars.values());
        sqlQuery.bindVars.addAll(bindVarsByIndex.values());
        if (debug)
            tdlog(Level.FINEST, "starting sendQuery");
        sqlQuery.setFlags.addAll(pendingSetStmts);

        if (types != null)
            sqlQuery.types.addAll(types);

        if (isRetry > 0) {
            sqlQuery.hasRetry = true;
            sqlQuery.retry = isRetry;
        }

        tdlog(Level.FINEST,
              "sendQuery sql='%s' isBegin=%b skipNRows=%d nretry=%d doAppend=%b",
              sql, isBegin, skipNRows, nretry, doAppend);

        /* SKIP_INTRANS_RESULTS optimization is disabled temporarily
           in cdb2jdbc to make executeUpdate() work. */
        /*
        if (isBegin)
            sqlQuery.features.add(CDB2ClientFeatures.SKIP_INTRANS_RESULTS_VALUE);
        */

        sqlQuery.features.add(CDB2ClientFeatures.ALLOW_MASTER_DBINFO_VALUE);
        sqlQuery.features.add(CDB2ClientFeatures.SSL_VALUE);
        if (nretry >= myDbHosts.size())
            sqlQuery.features.add(CDB2ClientFeatures.ALLOW_QUEUING_VALUE);

        if (nretry >= ((myDbHosts.size() * 2) - 1) && dbHostConnected ==
                masterIndexInMyDbHosts)
            sqlQuery.features.add(CDB2ClientFeatures.ALLOW_MASTER_EXEC_VALUE);

        sqlQuery.cnonce = cnonce;
        
        sqlQuery.reqInfo = new Cdb2ReqInfo();
        sqlQuery.reqInfo.timestampus = timestampus;
        sqlQuery.reqInfo.numretries = nretry;

        if (snapshotFile > 0) { 
            tdlog(Level.FINEST, "Setting hasSnapshotInfo to true because snapshotFile is %d", snapshotFile);
            sqlQuery.hasSnapshotInfo = true;
            sqlQuery.file = snapshotFile;
            sqlQuery.offset = snapshotOffset;
        }
        else {
            tdlog(Level.FINEST, "Not setting snapshotInfo");
        }

        if (skipNRows > 0) {
            sqlQuery.hasSkipRows = true;
            sqlQuery.skipRows = skipNRows;
        }

        int length = protocol.pack(query);
        NewSqlHeader nsh = new NewSqlHeader(RequestType.CDB2_REQUEST_CDB2QUERY,
                0, 0, length);

        try {
            io.write(nsh.toBytes());
            byte[] payload = protocol.write(io.getOut());
            io.flush();

            if (inTxn && doAppend && snapshotFile > 0) {
                queryList.add(new QueryItem(payload, sql, isRead));
            }
            tdlog(Level.FINEST, "sendQuery returns a good rcode");
            return true;
        } catch (IOException e) {
            last_non_logical_err = e;
            tdlog(Level.FINE, "sendQuery unable to write to %s/%s", myDbName, myDbCluster);
            driverErrStr = "Failed sending query.";
            return false;
        }
    }

    private NewSqlHeader readNsh() {
        try {
            byte[] res = new byte[NewSqlHeader.BYTES_NEEDED];
            NewSqlHeader nsh = null;
            do {
                if (io.read(res) != NewSqlHeader.BYTES_NEEDED) // unexpected
                                                                // response
                                                                // from server
                    return null;
                nsh = NewSqlHeader.fromBytes(res);

                if (nsh != null) {
                    ack = (nsh.type == Sqlresponse.ResponseHeader.SQL_RESPONSE_PING_VALUE);
                    /* Server requires SSL. */
                    if (nsh.type == Sqlresponse.ResponseHeader.SQL_RESPONSE_SSL_VALUE)
                        return nsh;
                }

            } while (nsh == null || nsh.length == 0); // if error occurs when
                                                        // constructing
                                                        // NewSqlHeader from
                                                        // bytes read,
                                                        // or length returned is
                                                        // 0 (a
                                                        // heartbeat packet),
                                                        // try again.
            return nsh;
        } catch (IOException e) {
            last_non_logical_err = e;
            driverErrStr = "Failed reading newsql header.";
            return null;
        }
    }

    private byte[] readRaw(int len) {
        try {
            byte[] res = new byte[len];

            int actual = io.read(res);
            if (actual != len) {
                logger.log(Level.WARNING, "Can't get correct length: " + len);
                driverErrStr = "Can't get correct length. Expect "
                    + len + ". Got " + actual + ".";
                return null;
            }
            return res;
        } catch (IOException e) {
            last_non_logical_err = e;
            driverErrStr = "Failed reading records.";
            return null;
        }
    }

    /* shortcut for reading a sql response */
    private Cdb2SqlResponse readRecord() {
        tdlog(Level.FINEST, "readRecord starting");
        NewSqlHeader nsh = readNsh();
        if (nsh != null) {
            byte[] raw = readRaw(nsh.length);
            if (raw != null) {
                Cdb2SqlResponse rsp = null;
                try {
                    rsp = protocol.unpackSqlResp(raw);
                    tdlog(Level.FINEST, "readRecord respType is %d", rsp.respType);
                } catch (IOException ioe) {
                    last_non_logical_err = ioe;
                    tdlog(Level.FINEST, "unpackSqlResp returns null");
                }
                return rsp;
            }
        }
        return null;
    }

    private void clearResp() {
        lastResp = null;
        firstResp = null;
    }

    @Override
    public void clearResult() {
        while (!skipDrain && next_int() == Errors.CDB2_OK) {
        }
        clearResp();
    }

    private boolean is_retryable(int err_val) {
        switch(err_val) {
            case Errors.CDB2ERR_CHANGENODE:
            case Errors.CDB2ERR_NOMASTER:
            case Errors.CDB2ERR_TRAN_IO_ERROR:
            case Errors.CDB2ERR_REJECTED:
            case Sqlresponse.CDB2_ErrorCode.MASTER_TIMEOUT_VALUE:
                return isHASql;
            default:
                return false;
        }
    }

    private void cleanup_query_list() {
        tdlog(Level.FINEST, "In cleanup_query_list");
        readIntransResults = true;
        snapshotFile = 0;
        snapshotOffset = 0;
        isRetry = 0;
        errorInTxn = 0;
        inTxn = false;
        queryList.clear();
    }

    @Override
    public int runStatement(String sql) {
        return runStatement(sql, null);
    }

    /* Sql interface to these */
    private boolean isClientOnlySetCommand(String sql) {
        String tokens[] = sql.split(" ");

        if (tokens.length < 1)
            return false;

        // Debug
        if (tokens[1].equals("debug")) {
            if (tokens.length == 3) {
                if (    tokens[2].equals("on") || 
                        tokens[2].equals("true") || 
                        tokens[2].equals("yes")) {
                    setDebug(true);
                } else {
                    setDebug(false);
                }
            }
            return true;
        }

        // max_retries
        if (tokens[1].equals("max_retries")) {
            if (tokens.length == 3) {
                int max = Integer.parseInt(tokens[2]);
                setMaxRetries(max);
            }
            return true;
        }

        // ssl
        boolean sslChanged = false;
        if (tokens[1].equals("ssl_mode")) {
            if (tokens.length < 3)
                return false;
            setSSLMode(tokens[2]);
            sslChanged = true;
        } else if (tokens[1].equals("key_store")) {
            if (tokens.length < 3)
                return false;
            setSSLCrt(tokens[2]);
            sslChanged = true;
        } else if (tokens[1].equals("key_store_password")) {
            if (tokens.length < 3)
                return false;
            setSSLCrtPass(tokens[2]);
            sslChanged = true;
        } else if (tokens[1].equals("key_store_type")) {
            if (tokens.length < 3)
                return false;
            setSSLCrtType(tokens[2]);
            sslChanged = true;
        } else if (tokens[1].equals("trust_store")) {
            if (tokens.length < 3)
                return false;
            setSSLCA(tokens[2]);
            sslChanged = true;
        } else if (tokens[1].equals("trust_store_password")) {
            if (tokens.length < 3)
                return false;
            setSSLCAPass(tokens[2]);
            sslChanged = true;
        } else if (tokens[1].equals("trust_store_type")) {
            if (tokens.length < 3)
                return false;
            setSSLCAType(tokens[2]);
            sslChanged = true;
        } else if (tokens[1].equals("crl")) {
            if (tokens.length < 3)
                return false;
            setSSLCRL(tokens[2]);
            sslChanged = true;
        }

        /* Refresh connection if SSL config has changed. */
        if (sslChanged) {
            sslerr = false;
            if (opened)
                closeNoException();
            return true;
        }

        return false;
    }

    private int runStatementInt(String sql, List<Integer> types) {
        int commitSnapshotFile = 0;
        int commitSnapshotOffset = 0;
        List<QueryItem> commitQueryList = null;
        boolean isHASqlCommit = false;
        int commitIsRetry = 0;
        int rc;

        sql = sql.trim();
        String lowerSql = sql.toLowerCase();

        while (next_int() == Errors.CDB2_OK)
            ;

        rowsRead = 0;

        tdlog(Level.FINE, "[running sql] %s", sql);

        if (lowerSql.startsWith("set")) {
            if (isClientOnlySetCommand(lowerSql)) {
                tdlog(Level.FINEST, "Added client-only set command %s", sql);
            } else {
                addSetStatement(sql);
                tdlog(Level.FINEST, "Added '%s' to sets size is %d uuid is %s",
                      sql, pendingSetStmts.size(), uuid);

                // HASql sessions need the file & offset from begin
                String hasql[] = lowerSql.split("hasql");
                if (hasql.length == 2) {
                    String ignoreSpace = hasql[1].replaceAll(" ","");
                    if (ignoreSpace.equals("on")) {
                        tdlog(Level.FINEST, "Set isHASql to true");
                        isHASql = true;
                    }
                    else {
                        tdlog(Level.FINEST, "Set isHASql to false");
                        isHASql = false;
                    }
                }
            }
            return 0;
        }

        boolean is_begin = false, is_commit = false, is_rollback = false;

        if (lowerSql.equals("begin"))
            is_begin = true;
        else if (lowerSql.equals("commit"))
            is_commit = true;
        else if (lowerSql.equals("rollback")) {
            is_commit = true;
            is_rollback = true;
        } else if (lowerSql.startsWith("select")
                || lowerSql.startsWith("explain")
                || lowerSql.startsWith("with")
                || lowerSql.startsWith("get")
                || lowerSql.startsWith("exec")) {
            isRead = true;
        } else {
            isRead = false;
        }

        if ((is_begin && inTxn) || (is_commit && !inTxn)) {
            last_non_logical_err = null;
            tdlog(Level.FINEST,
                  "returning wrong-handle-state, is_begin=%b is_commit=%b inTxn=%b",
                  is_begin, is_commit, inTxn);
            driverErrStr = "Wrong sql handle state";
            last_non_logical_err = null;
            return Errors.CDB2ERR_BADSTATE;
        }

        if (!inTxn) {
            snapshotFile = 0;
            snapshotOffset = 0;
            isRetry = 0;
            stringCnonce = UUID.randomUUID().toString();
            cnonce = stringCnonce.getBytes();
        }
        retryAll = false;

        // If we've already added this query onto the query-list 
        // we don't want to run the last
        boolean runLast = true;

        int retry = 0;
        boolean sent = false;
        for ( ; retry < maxretries && !sslerr; ++retry) {
            tdlog(Level.FINEST, "executing retry loop with retry %d", retry);
            firstRecordRead = false;

            isBeforeFirst = true;
            isFirst = false;
            isAfterLast = false;

            /* Add wait if we have tried on all the nodes. */
            if (retry > myDbHosts.size()) {
                if (!isHASql && retry > minretries) {
                    driverErrStr = "Can't connect to db.";
                    return Errors.CDB2ERR_CONNECT_ERROR;
                }
                try {
                    int sleepms = (100 * (retry - myDbHosts.size() + 1));
                    if (sleepms > 1000) {
                        sleepms = 1000;
                        tdlog(Level.FINE, "Sleeping on retry %d", retry);
                    }
                    Thread.sleep(sleepms);
                } catch (InterruptedException e) {
                    tdlog(Level.WARNING, "Error while waiting for nodes");
                }
            }

            if (dbHostConnected < 0) { /* connect to a node */
                if (is_rollback) {
                    readIntransResults = true;
                    snapshotFile = 0;
                    snapshotOffset = 0;
                    isRetry = 0;
                    errorInTxn = 0;
                    inTxn = false;
                    queryList.clear();
                    tdlog(Level.FINEST, "Returning 0 on host-not-connected for rollback");
                    return 0;
                }
                tdlog(Level.FINEST, "Connecting on retry ");
                if (!open()) {
                    tdlog(Level.FINE, "Connection open error");
                    continue;
                }
                tdlog(Level.FINEST, "Connected to %s", dbHostConnected);

                if (retry > 0 && !is_begin) {
                    retryAll = true;
                    int retryrc = retryQueries(retry, runLast);

                    if (retryrc < 0) {
                        if (inTxn)
                            errorInTxn = retryrc;
                        tdlog(Level.FINE, "Can't retry query, retryrc = %d", retryrc);
                        return retryrc;
                    } 
                    else if (retryrc > 0) {
                        tdlog(Level.FINEST, "retryQueries returns %d", retryrc);
                        closeNoException();
                        retryAll = true;
                        continue;
                    }
                }
            }

            clearResp();
            lastSql = sql;
            timestampus = System.currentTimeMillis() * 1000L;

            if (!inTxn || is_begin) {
                sent = sendQuery(sql, types, is_begin, 0, retry, 
                        is_begin ? false : runLast);
            } else {
                sent = sendQuery(sql, types, false, 0, 0, runLast);
            }

            if (!sent) {
                driverErrStr = "Can't send query to the db.";
                retryAll = true;
                closeNoException();
                tdlog(Level.FINER, "Continuting on !sendQuery");
                continue;
            }

            // We've appended this statement to the querylist
            runLast = false;

            int errVal = errorInTxn;
            boolean _readIntransResults = readIntransResults;

            do { /* poor man's goto in java */
                if (is_commit || is_rollback) {
                    if (is_commit && snapshotFile > 0) {
                        tdlog(Level.FINEST, "Saving snapshot file info for hasql commit");
                        commitSnapshotFile = snapshotFile;
                        commitSnapshotOffset = snapshotOffset;
                        commitIsRetry = isRetry;
                        commitQueryList = queryList;
                        queryList = new ArrayList<QueryItem>();
                        isHASqlCommit = true;
                    }
                    readIntransResults = true;
                    snapshotFile = 0;
                    snapshotOffset = 0;
                    isRetry = 0;
                    errorInTxn = 0;
                    inTxn = false;
                    queryList.clear();

                    if (!_readIntransResults) {
                        if (errVal != 0) {
                            if (is_rollback) {
                                tdlog(Level.FINER, "Rollback returning 0 on errVal %d", errVal);
                                return 0;
                            } else {
                                tdlog(Level.FINER, "Returning errVal %d on commit", errVal);
                                return errVal;
                            }
                        }
                    } else if (errVal != 0) {
                        tdlog(Level.FINEST, "Commit errVal is %d is_rollback=%b readIntransResults=%b",
                              errVal, is_rollback, _readIntransResults);
                        /* With read_intrans_results on, we need to read the 1st response
                           of commit/rollback even if there is an in-trans error. */
                        break;
                    } else {
                        tdlog(Level.FINEST, "Commit errVal (2) is %d is_rollback=%b readIntransResults=%b",
                              errVal, is_rollback, _readIntransResults);
                    }
                }

                if (errVal != 0) {
                    tdlog(Level.FINER, "Returning errVal %d is_rollback=%b", errVal, is_rollback);
                    return is_rollback? 0 : errVal;
                }

                if (!readIntransResults && !isRead && (inTxn || !isHASql)) {
                    tdlog(Level.FINER, "readIntransResults is disabled and !isRead: %b returning 0", !isRead);
                    return 0;
                }
            } while (false);

            NewSqlHeader nsh = null;
            byte[] raw = null;

            tdlog(Level.FINEST, "reading results");
            // Read results
            if ((nsh = readNsh()) == null || (raw = readRaw(nsh.length)) == null) {
                tdlog(Level.FINEST, "Failure to read: nsh=%s raw=%s", nsh, Arrays.toString(raw));
                // Read error
                if (errVal != 0) {
                    if (is_rollback) {
                        tdlog(Level.FINER, "returning 0 for null readNsh / readRaw on rollback errVal=%d", errVal);
                        return 0;
                    }
                    else if (is_retryable(errVal) && (snapshotFile > 0 ||
                                (!inTxn && !is_commit) || commitSnapshotFile > 0)) {
                        tdlog(Level.FINER, "continuing on retryable error %d for null readNsh", errVal);
                        errorInTxn = 0;
                        closeNoException();
                        retryAll = true;
                        if (commitSnapshotFile > 0) {
                            tdlog(Level.FINER,
                                    "Resetting txn state info on commit, isHASql=%b lsn=[%d][%d]",
                                    isHASql, commitSnapshotFile, commitSnapshotOffset);
                            inTxn = true;
                            snapshotFile = commitSnapshotFile;
                            snapshotOffset = commitSnapshotOffset;
                            isRetry = commitIsRetry;
                            queryList = commitQueryList;
                            commitQueryList = null;
                            commitSnapshotFile = 0;
                        }
                        continue;
                    }
                    else {
                        tdlog(Level.FINER, "continuing on retryable error %d for null readNsh", errVal);
                        if (is_commit) {
                            cleanup_query_list();
                        } 
                        return errVal;
                    }
                }

                if (!is_commit || snapshotFile > 0) {
                    closeNoException();
                    retryAll = true;
                    tdlog(Level.FINER, "continue with retryAll true on null readNsh / readRaw");
                    continue;
                }

                closeNoException();

                if (isHASql || commitSnapshotFile > 0) {
                    if (commitSnapshotFile > 0) {
                        tdlog(Level.FINER,
                              "Resetting txn state info on commit, isHASql=%b lsn=[%d][%d]",
                              isHASql, commitSnapshotFile, commitSnapshotOffset);
                        inTxn = true;
                        snapshotFile = commitSnapshotFile;
                        snapshotOffset = commitSnapshotOffset;
                        isRetry = commitIsRetry;
                        queryList = commitQueryList;
                        commitQueryList = null;
                        commitSnapshotFile = 0;
                    }
                    retryAll = true;
                    tdlog(Level.FINER, "continue with retryAll true for isHASqlCommit && null readNsh / readRaw");
                    continue;
                }

                if (isHASqlCommit) {
                    cleanup_query_list();
                }
                driverErrStr = "Can't read response from the db.";
                tdlog(Level.FINER,
                      "Returning connect-error, is_commit=%b snapshotFile=%d is_rollback=%b",
                      is_commit, snapshotFile, is_rollback);
                return Errors.CDB2ERR_CONNECT_ERROR;
            }

            if (nsh.type == Sqlresponse.ResponseHeader.SQL_RESPONSE_SSL_VALUE) {
                peersslmode = PEER_SSL_MODE.PEER_SSL_REQUIRE;
                trySSL();
                /* Decrement retry counter: It is not a real retry. */
                --retry;
                continue;
            }

            // Dbinfo .. go to new node
            if (nsh.type == Sqlresponse.ResponseHeader.DBINFO_RESPONSE_VALUE) {
                tdlog(Level.FINEST, "got dbinfo");
                try {
                    Cdb2DbInfoResponse dbinfo = protocol.unpackDbInfoResp(raw);
                    ArrayList<String> validHosts = new ArrayList<String>();
                    ArrayList<Integer> validPorts = new ArrayList<Integer>();
                    try {
                        DatabaseDiscovery.dbInfoQuery(this,
                                dbinfo, myDbName, myDbNum,
                                null, 0, validHosts, validPorts);
                    } catch (IOException e) {
                        validHosts.clear();
                    }

                    if (validHosts.size() > 0) {
                        /* purge old hosts. add the new ones. */
                        myDbHosts.clear();
                        myDbHosts.addAll(validHosts);
                        myDbPorts.clear();
                        myDbPorts.addAll(validPorts);
                    }
                } catch (IOException ioe) {
                    last_non_logical_err = ioe;
                }

                retryAll = true;
                /* refresh dbHostIdx to reset load balance policy */
                dbHostIdx = -1;
                closeNoException();

                tdlog(Level.FINER, "continuing with retryAll=true on nsh.type=DBINFO_RESPONSE_VALUE");
                continue;
            }

            try {
                firstResp = protocol.unpackSqlResp(raw);
                tdlog(Level.FINEST, "firstResp!=null");
                if (errVal != 0) {

                    if (is_rollback) {
                        tdlog(Level.FINER, "returning 0 for rollback after unpack for errVal=%d", errVal);
                        return 0;
                    }
                    else {
                        if (isHASqlCommit) {
                            cleanup_query_list();
                        }
                        tdlog(Level.FINER, "returning errval for after unpack for errVal=%d", errVal);
                        return errVal;
                    }
                } else {
                    tdlog(Level.FINEST, "firstResp!=null but errVal == 0");
                }
            } catch (IOException ioe) {
                // error unpacking
                //closeNoException();
                tdlog(Level.FINEST, "firstResp==null");
                last_non_logical_err = ioe;
                if (errVal != 0) {

                    if (is_rollback) {
                        tdlog(Level.FINER, "Returning 0 on rollback for errval %d for null firstResp", errVal);
                        return 0;
                    }
                    else if (is_retryable(errVal) && (snapshotFile > 0 ||
                                (!inTxn && !is_commit) || commitSnapshotFile > 0)) {
                        errorInTxn = 0;
                        closeNoException();
                        retryAll = true;
                        if (commitSnapshotFile > 0) {
                            tdlog(Level.FINER,
                                    "Resetting txn state info on commit, isHASql=%b lsn=[%d][%d]",
                                    isHASql, commitSnapshotFile, commitSnapshotOffset);
                            inTxn = true;
                            snapshotFile = commitSnapshotFile;
                            snapshotOffset = commitSnapshotOffset;
                            isRetry = commitIsRetry;
                            queryList = commitQueryList;
                            commitQueryList = null;
                            commitSnapshotFile = 0;
                        }
                        tdlog(Level.FINER, "Retrying for failed protocol unpack errval=%d", errVal);
                        continue;
                    }
                    else {
                        if (isHASqlCommit) {
                            cleanup_query_list();
                        }
                        tdlog(Level.FINEST, "Returning non-retryable error %d for null firstresp", errVal);
                        return errVal;
                    }
                }

                if (!is_commit || snapshotFile > 0) {
                    closeNoException();
                    retryAll = true;
                    tdlog(Level.FINER,
                          "Continue with true retryAll for non-commit, null firstResp and 0 errVal");
                    continue;
                } 
               
                driverErrStr = "Timeout while reading response from server.";

                tdlog(Level.FINER,
                      "Returning IO_ERROR for firstResp=null errVal=0 is_commit=%b snapshotFile=%d is_rollback=%b",
                      is_commit, snapshotFile, is_rollback);

                if (isHASqlCommit) {
                    cleanup_query_list();
                }
                return Errors.CDB2ERR_IO_ERROR;
            }

            // no hints ..

            if ((firstResp.errCode == Sqlresponse.CDB2_ErrorCode.MASTER_TIMEOUT_VALUE ||
                firstResp.errCode == Errors.CDB2ERR_CHANGENODE) && (snapshotFile > 0 ||
                (!inTxn && !is_commit) || commitSnapshotFile > 0)) {
                closeNoException();
                retryAll = true;
                if(commitSnapshotFile > 0) {
                    tdlog(Level.FINEST, "Resetting txn info for ishasql commit MASTER_TIMEOUT_VALUE");
                    inTxn = true;
                    snapshotFile = commitSnapshotFile;
                    snapshotOffset = commitSnapshotOffset;
                    isRetry = commitIsRetry;
                    queryList = commitQueryList;
                    commitQueryList = null;
                    commitSnapshotFile = 0;
                }
                tdlog(Level.FINER,
                      "Continue with true retryAll for firstResp.errCode=MASTER_TIMEOUT_VALUE");
                continue;
            }

            if (is_begin)
                this.inTxn = true;
            else if (!isHASqlCommit && (is_commit || is_rollback)) {
                this.inTxn = false;
                cleanup_query_list();
            }

            // This could fail .. keep the snapshotFile until I know one way or the other.
            if (is_commit) {
                snapshotFile = 0;
                snapshotOffset = 0;
                isRetry = 0;
            }


            /* Column names */
            if (firstResp.respType == 1) {
                /* Handle rejects from server. */
                tdlog(Level.FINEST, "firstResp.respType==1");
                if (is_retryable(firstResp.errCode) && (snapshotFile > 0 ||
                            (!inTxn && !is_commit) || commitSnapshotFile > 0)) {
                    closeNoException();
                    retryAll = true;
                    if (commitSnapshotFile > 0) {
                        inTxn = true;
                        snapshotFile = commitSnapshotFile;
                        snapshotOffset = commitSnapshotOffset;
                        isRetry = commitIsRetry;
                        queryList = commitQueryList;
                        commitQueryList = null;
                        commitSnapshotFile = 0;
                    }
                    tdlog(Level.FINER, "firstResp.columnNames errCode=%d", firstResp.errCode);
                    continue;
                }

                if (firstResp.errCode != 0) {
                    rc = convert_rc(firstResp.errCode);
                    if (inTxn) {
                        errorInTxn = rc;
                    }
                    if (isHASqlCommit) {
                        cleanup_query_list();
                    }
                    tdlog(Level.FINER,
                          "Returning rc=%d on firstResp.errCode=%d",
                          rc, firstResp.errCode);
                    return rc;
                }

                int nxtrc = next_int();
                if (nxtrc == Errors.CDB2_OK || nxtrc == Errors.CDB2_OK_DONE) {
                    int rtnrc = convert_rc(firstResp.errCode);
                    if (isHASqlCommit) {
                        cleanup_query_list();
                    }
                    tdlog(Level.FINER, "nxtrc is "+nxtrc+" returning " + rtnrc + " is_rollback=" + is_rollback);
                    return is_rollback ? 0 : rtnrc;
                }

                // this happens with 'begin'
                if (isHASql && (((is_retryable(nxtrc) && snapshotFile>0) || is_begin) || 
                            (io == null && ((inTxn && snapshotFile > 0) || commitSnapshotFile > 0)))) {
                    closeNoException();

                    if (commitSnapshotFile > 0) {
                        inTxn = true;
                        snapshotFile = commitSnapshotFile;
                        snapshotOffset = commitSnapshotOffset;
                        isRetry = commitIsRetry;
                        queryList = commitQueryList;
                        commitQueryList = null;
                        commitSnapshotFile = 0;
                    }

                    if (is_begin)
                        cleanup_query_list();

                    tdlog(Level.FINER, "Retrying on nxtrc=%d is_begin=%b", nxtrc, is_begin);
                    retryAll = true;
                    continue;
                }

                if (isHASqlCommit) {
                    cleanup_query_list();
                }
                tdlog(Level.FINER, "Returning nxtrc=%d is_begin=%b isHASql=%b",
                      nxtrc, is_begin, isHASql);
                return convert_rc(nxtrc);
            } else {
                // XXX I could paper over this with the api, but i want to see what is 
                // happening first
                tdlog(Level.FINEST, "XXX FAIL .. firstResp.respType=%d", firstResp.respType);
            }

        } /* end of the big for loop */

        if (retry >= maxretries && !sent)
            driverErrStr = "Maximum number of retries done.";
        if (isHASqlCommit) {
            cleanup_query_list();
        }

        // We've run out of retries: if this was a begin, set inTxn to false
        if (is_begin)
            inTxn = false;

        tdlog(Level.FINER, "Maximum retries done: returning IO_ERROR, is_rollback=%b", is_rollback);
        return is_rollback ? 0 : Errors.CDB2ERR_TRAN_IO_ERROR;
    }

    @Override
    public synchronized int runStatement(String sql, List<Integer> types) {
        int rc, commit_rc;

        if (temp_trans && inTxn) {
            runStatementInt("rollback", null);
        }

        temp_trans = false;

        if (isHASql && !inTxn) {
            String locase = sql.toLowerCase().trim();

            if (!locase.startsWith("set") && !locase.startsWith("begin") &&
                !locase.startsWith("commit") && !locase.startsWith("rollback")) {

                rc = runStatementInt("begin", null);
                if (rc != 0) {
                    return rc;
                }
                temp_trans = true;
            }
        }

        rc = runStatementInt(sql, types);

        if (temp_trans && !isRead) {
            if (rc == 0) {
                commit_rc = runStatementInt("commit", null);
                rc = commit_rc;
            }
            else {
                runStatementInt("rollback", null);
            }
            temp_trans = false;
        }

        return rc;
    }

    @Override
    public void runStatementWithException(String sql) {
        runStatementWithException(sql, null);
    }

    @Override
    public void runStatementWithException(String sql, List<Integer> types) {
        int ret = runStatement(sql, types);
        if (ret == Errors.CDB2ERR_CONNECT_ERROR)
            throw new UnableToEstablishConnectionException(myDbName);
        if (ret == Errors.CDB2ERR_IO_ERROR)
            throw new Cdb2IOException();
        if (ret != 0)
            throw new Cdb2SqlRuntimeException(errorString(), ret);
    }

    private void ack() {
        ack = false;
        NewSqlHeader nsh = new NewSqlHeader(
                Sqlresponse.ResponseHeader.SQL_RESPONSE_PONG_VALUE, 0, 0, 0);
        try {
            io.write(nsh.toBytes());
            io.flush();
        } catch (IOException e) {
            last_non_logical_err = e;
            logger.log(Level.WARNING,
                    "Unable to ack to " + myDbName + "/" + myDbCluster, e);
        }
    }

    /* Clear the ack flag */
    public void clearAck() {
        ack = false;
    }

    @Override
    public boolean nextWithException() {

        int ret = next();

        if (ret == 0)
            return true;
        if (ret == 1)
            return false;

        throw new Cdb2SqlRuntimeException(errorString(), ret);
    }

    private int convert_rc(int rc) {
        return rc == 1 ? Errors.CDB2ERR_DUPLICATE : rc;
    }

    @Override
    public synchronized int next() {

        if (isFirst)
            isFirst = false;

        if (inTxn && !readIntransResults && !isRead) {
            isAfterLast = true;
            return Errors.CDB2_OK_DONE;
        }

        int rc;

        if (lastResp != null && !firstRecordRead) {
            last_non_logical_err = null;
            firstRecordRead = true;

            if (lastResp.respType == 2) {
                rc = lastResp.errCode;
            } else if (lastResp.respType == 3) {
                mergeSetStatements();
                rc = Errors.CDB2_OK_DONE;
            } else {
                rc = Errors.CDB2ERR_IO_ERROR;
            }
        } else {
            rc = next_int();
        }

        if (rc == Errors.CDB2_OK_DONE)
            isAfterLast = true;
        else if (rc == Errors.CDB2_OK) {
            if (isBeforeFirst) {
                isBeforeFirst = false;
                isFirst = true;
            }
        }

        return rc;
    }

    private int next_int() {
        boolean skip_to_open = false;
        boolean continue_retry = false;
        //boolean begin_retry = false;
        int nretry = 0;

        if (debug)
            tdlog(Level.FINEST, "starting next_int");

        if (ack) ack();

readloop:
        do {
            continue_retry = false;
            tdlog(Level.FINEST, "Enter readloop with skip_to_open=%b", skip_to_open);
            if (!skip_to_open) {
                if (firstResp == null) {
                    tdlog(Level.FINEST, "next_int: returning OK_DONE for null firstResp");
                    return Errors.CDB2_OK_DONE;
                }

                if (firstResp.errCode != 0) {
                    last_non_logical_err = null;
                    tdlog(Level.FINEST, "next_int: returning firstResp.errCode %d", firstResp.errCode);
                    return firstResp.errCode;
                }
                if (lastResp != null) {
                    last_non_logical_err = null;
                    if (lastResp.respType == 3) {
                        tdlog(Level.FINEST, "next_int: returning OK_DONE for lastResp.respType = 3");
                        return Errors.CDB2_OK_DONE;
                    }
                    if (lastResp.respType == 2 && lastResp.errCode != 0) {
                        int rc = convert_rc(lastResp.errCode);
                        if (inTxn)
                            errorInTxn = rc;
                        tdlog(Level.FINEST, "next_int: returning %d for lastResp.respType=2(1) inTxn=%b", rc, inTxn);
                        return rc;
                    }
                }
            }
            else {
                tdlog(Level.FINEST, "skipped first part of readloop because skip_to_open is %b", skip_to_open);
            }

            if (skip_to_open || (lastResp = readRecord()) == null) {
                driverErrStr = "Timeout while reading response from server";
                closeNoException();

                if (in_retry) {
                    driverErrStr = "Can't connect to db.";
                    tdlog(Level.FINEST, "next_int: returning CONNECT_ERROR because in_retry");
                    return Errors.CDB2ERR_CONNECT_ERROR;
                }

                skip_to_open = false;
                while (snapshotFile > 0 && nretry < maxretries && !sslerr) {
                    nretry++;
                    if (nretry > myDbHosts.size()) {
                        int tmsec;
                        tmsec = (nretry - myDbHosts.size()) * 100;
                        if (tmsec > 1000)
                            tmsec = 1000;
                        tdlog(Level.FINEST, "Sleeping on read retry %d", nretry);
                        try {
                            Thread.sleep(tmsec);
                        }
                        catch (InterruptedException e) { }
                    }
                    if (!open()) {
                        tdlog(Level.FINEST, "next_int: retrying with snapshotFile=%d", snapshotFile);
                        continue;
                    }
                    if (!retryQueriesAndSkip(nretry, rowsRead)) {
                        closeNoException();
                        continue;
                    }
                    // Must continue_retry or this will exit this loop
                    continue_retry = true;
                    tdlog(Level.FINEST, "continuing readloop on reconnect & retryQueries");
                    continue readloop;
                } /* max number of replays done */

                if (snapshotFile <= 0 || nretry >= maxretries) {
                    /* if no HA txn or max number of HA replays done */
                    tdlog(Level.FINEST,
                          "next_int: returning CONNECT_ERROR(2) with snapshotFile=%d and maxretries=%d",
                          snapshotFile, maxretries);
                    return Errors.CDB2ERR_CONNECT_ERROR;
                }
            }
            //begin_retry = false;

            if (lastResp.hasSnapshotInfo) {
                snapshotFile = lastResp.file;
                snapshotOffset = lastResp.offset;
                tdlog(Level.FINEST,
                      "next_int: set snapshot lsn to [%d][%d]",
                      snapshotFile, snapshotOffset);
            }

            /*
            if (lastResp.respType == 3) {
                last_non_logical_err = null;
                // This only happens in a BEGIN which can't get a durable lsn 
                // from the master
                if (is_retryable(lastResp.errCode)) {
                    //begin_retry = true;
                    tdlog(Level.FINEST, "next_int: returning retryable rcode" + 
                            lastResp.errCode + " on begin");
                    //closeNoException();
                    //skip_to_open = true;
                    //continue readloop;
                    return lastResp.errCode;
                }
            }
            */

            if (lastResp.respType == 2) { // COLUMN_VALUES
                if (is_retryable(lastResp.errCode) && snapshotFile > 0) {
                    tdlog(Level.FINEST,
                          "next_int: continuing retryable rcode %d at [%d][%d]",
                          lastResp.errCode, snapshotFile, snapshotOffset);
                    closeNoException();
                    skip_to_open = true;
                    continue readloop;
                }

                ++rowsRead;

                int rc = convert_rc(lastResp.errCode);
                if (inTxn) {
                    errorInTxn = rc;
                }
                tdlog(Level.FINEST,
                      "next_int: returning %d for lastResp.respType=2(2) inTxn=%b",
                      rc, inTxn);
                return rc;
            }
        } while(skip_to_open || continue_retry);

        if (lastResp.respType == 3) {

            if (is_retryable(lastResp.errCode)) {
                int rc = lastResp.errCode;
                closeNoException();
                return rc;
            }

            mergeSetStatements();

            if (inTxn && lastResp.features != null) {
                for (int feature : lastResp.features) {
                    if (CDB2ServerFeatures.SKIP_INTRANS_RESULTS_VALUE == feature) {
                        readIntransResults = false;
                        break;
                    }
                }
            }
            tdlog(Level.FINEST,
                  "next_int: lastResp.respType is 3, lastResp.errCode=%d", lastResp.errCode);

            return Errors.CDB2_OK_DONE;
        }

        driverErrStr = "Can't connect to db.";
        tdlog(Level.FINEST, "Returning CONNECT_ERROR(3) from next_int");
        return Errors.CDB2ERR_CONNECT_ERROR;
    }

    @Override
    public String errorString() {
        if (firstResp == null)
            return driverErrStr;
        if (lastResp == null) {
            if (firstResp.errStr != null
                    && firstResp.errStr.length() > 0) {
                driverErrStr = firstResp.errStr;
                return firstResp.errStr;
            }
            return driverErrStr;
        }

        driverErrStr = lastResp.errStr;
        return lastResp.errStr;
    }

    @Override
    public boolean open() {
        boolean rc;
        if (opened) {
            tdlog(Level.FINEST, "Connection already open");
            return true;
        } else {
            rc = reopen(true);
            if (rc)
                sentClientInfo = false;
            tdlog(Level.FINEST, "Connection reopened returned %b", rc);
            return rc;
        }
    }

    private boolean trySSL() {
        boolean dossl = false;
        if (SSL_MODE.isRequired(sslmode)) {
            switch (peersslmode) {
                case PEER_SSL_UNSUPPORTED:
                    driverErrStr = "The database does not support SSL.";
                    sslerr = true;
                    return false;
                case PEER_SSL_ALLOW:
                case PEER_SSL_REQUIRE:
                    dossl = true;
                    break;
            }
        } else if (SSL_MODE.isPreferred(sslmode)) {
            switch (peersslmode) {
                case PEER_SSL_UNSUPPORTED:
                    dossl = false;
                    break;
                case PEER_SSL_ALLOW:
                case PEER_SSL_REQUIRE:
                    dossl = true;
                    break;
            }
        } else {
            switch (peersslmode) {
                case PEER_SSL_ALLOW:
                case PEER_SSL_UNSUPPORTED:
                    dossl = false;
                    break;
                case PEER_SSL_REQUIRE:
                    dossl = true;
                    break;
            }
        }

        sslerr = false;
        if (!dossl)
            return true;

        try {
            io = new SSLIO((SockIO)io, sslmode,
                           myDbName, sslNIDDbName,
                           sslcert, sslcerttype,
                           sslcertpass, sslca,
                           sslcatype, sslcapass,
                           sslcrl);
            sentClientInfo = false;
            return true;
        } catch (SSLHandshakeException she) {
            /* this is NOT retry-able. */
            last_non_logical_err = she;
            sslerr = true;
            return false;
        } catch (SSLException se) {
            /* this is retry-able. */
            last_non_logical_err = se;
            try { io.close(); } catch (IOException ioe) {}
            return false;
        }
    }

    private boolean reopen(boolean refresh_dbinfo_if_failed) {
        /* get the index of the preferred machine */
        if (prefIdx == -1 && prefmach != null) {
            prefIdx = myDbHosts.indexOf(prefmach);

            if (prefIdx != -1 && myPolicy != POLICY_RANDOM) {
                logger.log(Level.WARNING,
                        "Overwriting load balance policy to RANDOM " +
                        "because preferred machine is set.");
                myPolicy = POLICY_RANDOM;
            }
        }

        /* if the preferred machine is valid, and
           we're not on it, connect to it. */
        if (prefIdx != -1 && dbHostIdx != prefIdx) {
            io = new SockIO(myDbHosts.get(prefIdx),
                    myDbPorts.get(prefIdx), tcpbufsz, pmuxrte ? myDbName : null,
                    soTimeout, connectTimeout);
            if (io.open()) {
                try {
                    io.write("newsql\n");
                    io.flush();
                    if (!trySSL())
                        return false;
                    dbHostConnected = prefIdx;
                    dbHostIdx = prefIdx;
                    resetSetStatements();
                    opened = true;
                    return true;
                } catch (IOException e) {
                    last_non_logical_err = e;
                    logger.log(Level.SEVERE, "Unable to write newsql to master " 
                            + myDbHosts.get(dbHostIdx) + ":" + myDbPorts.get(dbHostIdx), e);
                    try { io.close(); } catch (IOException e1) {}
                    io = null;
                }
            }
        }

        if (myPolicy == POLICY_RANDOM && myDbHosts.size() > 0) {
            Random rnd = new Random(System.currentTimeMillis());
            dbHostIdx = Math.abs(rnd.nextInt()) % myDbHosts.size();
        } else if (myPolicy == POLICY_RANDOMROOM
                && dbHostIdx == -1 && numHostsSameRoom > 0) {
            Random rnd = new Random(System.currentTimeMillis());
            dbHostIdx = Math.abs(rnd.nextInt()) % numHostsSameRoom;
            /* connect to same room once */
            for (int i = 0; i != numHostsSameRoom; ++i) {
                int try_node = (dbHostIdx + i) % numHostsSameRoom;
                if (try_node == masterIndexInMyDbHosts
                        || myDbPorts.get(try_node) < 0
                        || try_node == dbHostConnected)
                    continue;

                io = new SockIO(myDbHosts.get(try_node), myDbPorts.get(try_node),
                                tcpbufsz, pmuxrte ? myDbName : null,
                                soTimeout, connectTimeout);
                if (io.open()) {
                    try {
                        io.write("newsql\n");
                        io.flush();
                        if (!trySSL())
                            return false;
                        dbHostConnected = try_node;
                        resetSetStatements();
                        opened = true;
                        return true;
                    } catch (IOException e) {
                        last_non_logical_err = e;
                        logger.log(Level.WARNING, "Unable to write newsql to "
                                + myDbHosts.get(try_node) + ":" + myDbPorts.get(try_node), e);
                        try { io.close(); } catch (IOException e1) { }
                        io = null;
                        continue;
                    }
                }
            }
            dbHostIdx = (numHostsSameRoom - 1);
        }

        /**
         * First, try slave nodes.
         */

        // last time we were at dbHostIdx, this time start from (dbHostIdx + 1)
        int start_req;
        if (dbHostIdx == myDbHosts.size())
            start_req = dbHostIdx = 0;
        else
            start_req = ++dbHostIdx;

        for (; dbHostIdx < myDbHosts.size(); ++dbHostIdx) {
            if (dbHostIdx == masterIndexInMyDbHosts
                    || myDbPorts.get(dbHostIdx) < 0
                    || dbHostIdx == dbHostConnected)
                continue;

            io = new SockIO(myDbHosts.get(dbHostIdx), myDbPorts.get(dbHostIdx),
                            tcpbufsz, pmuxrte ? myDbName : null,
                            soTimeout, connectTimeout);
            if (io.open()) {
                try {
                    io.write("newsql\n");
                    io.flush();
                    if (!trySSL())
                        return false;
                    dbHostConnected = dbHostIdx;
                    resetSetStatements();
                    opened = true;
                    return true;
                } catch (IOException e) {
                    last_non_logical_err = e;
                    logger.log(Level.WARNING, "Unable to write newsql to " 
                            + myDbHosts.get(dbHostIdx) + ":" + myDbPorts.get(dbHostIdx), e);
                    try { io.close(); } catch (IOException e1) { }
                    io = null;
                    continue;
                }
            }
        }

        // start over from offset 0
        for (dbHostIdx = 0; dbHostIdx < start_req; ++dbHostIdx) {
            if (dbHostIdx == masterIndexInMyDbHosts
                    || myDbPorts.get(dbHostIdx) < 0
                    || dbHostIdx == dbHostConnected)
                continue;

            io = new SockIO(myDbHosts.get(dbHostIdx), myDbPorts.get(dbHostIdx),
                            tcpbufsz, pmuxrte ? myDbName : null, 
                            soTimeout, connectTimeout);
            if (io.open()) {
                try {
                    io.write("newsql\n");
                    io.flush();
                    if (!trySSL())
                        return false;
                    dbHostConnected = dbHostIdx;
                    resetSetStatements();
                    opened = true;
                    return true;
                } catch (IOException e) {
                    last_non_logical_err = e;
                    logger.log(Level.WARNING, "Unable to write newsql to " 
                            + myDbHosts.get(dbHostIdx) + ":" + myDbPorts.get(dbHostIdx), e);
                    try { io.close(); } catch (IOException e1) { }
                    io = null;
                    continue;
                }
            }
        }

        /**
         * None of slave nodes works! Try master node.
         */
        if (masterIndexInMyDbHosts >= 0) {
            io = new SockIO(myDbHosts.get(masterIndexInMyDbHosts),
                            myDbPorts.get(masterIndexInMyDbHosts),
                            tcpbufsz, pmuxrte ? myDbName : null,
                            soTimeout, connectTimeout);
            if (io.open()) {
                try {
                    io.write("newsql\n");
                    io.flush();
                    if (!trySSL())
                        return false;
                    dbHostConnected = masterIndexInMyDbHosts;
                    dbHostIdx = masterIndexInMyDbHosts;
                    resetSetStatements();
                    opened = true;
                    return true;
                } catch (IOException e) {
                    last_non_logical_err = e;
                    logger.log(Level.SEVERE, "Unable to write newsql to master " 
                            + myDbHosts.get(masterIndexInMyDbHosts) + ":" + myDbPorts.get(masterIndexInMyDbHosts), e);
                    try { io.close(); } catch (IOException e1) { }
                    io = null;
                }
            }
        }

        /* Can't connect to any of the nodes.
           Re-check information about db. */
        if (!isDirectCpu && refresh_dbinfo_if_failed) {
            try {
                DatabaseDiscovery.getDbHosts(this, true);
                reopen(false);
            } catch (NoDbHostFoundException e) {
                logger.log(Level.SEVERE, "Failed to refresh dbinfo", e);
            }
        }

        dbHostConnected = -1;
        return false;
    }

    /**
     * Adds a 'set' statement to the pending list.
     */
    private void addSetStatement(String statement) {
        removeSetStatement(statement);
        pendingSetStmts.add(statement);
    }

    /**
     * Removes a 'set' statement from the pending list.
     */
    private void removeSetStatement(String statement) {
        pendingSetStmts.remove(statement);
    }

    /**
     * Caches the already sent 'set' statements, so they can be re-sent in the case of a
     * disconnection.
     */
    private void mergeSetStatements() {
        for (String setStatement : pendingSetStmts) {
            sentSetStmts.remove(setStatement);
            sentSetStmts.add(setStatement);
        }

        pendingSetStmts.clear();
    }

    /**
     * Restores the cached 'set' statements, in order to be sent again after a reconnection.
     */
    private void resetSetStatements() {
        // Make a copy of the pending statements before clearing the list
        List<String> pendingStatementsCopy = new LinkedList<String>(pendingSetStmts);

        pendingSetStmts.clear();
        pendingSetStmts.addAll(sentSetStmts); // Add the sent statements

        // Add the statements that were pending before the disconnection happened
        for (String setStatement : pendingStatementsCopy) {
            addSetStatement(setStatement);
        }
    }

    private void closeNoException() {
        if (debug)
            tdlog(Level.FINEST, "starting closeNoException");
        try {
            close();
        } catch (IOException e) {
            tdlog(Level.WARNING, "Unable to close connection to %s/%s", myDbName, myDbCluster);
        }
    }

    @Override
    public void close() throws IOException {

        if (opened == true) {
            synchronized (lock) {
                if (opened == true) {
                    if (ack && clearAckOnClose) {
                        ack();
                    }

                    opened = false;
                    dbHostConnected = -1;
                    clearResp();

                    if (io != null) {
                        io.close();
                        io = null;
                    }
                }
            }
        }
    }

    @Override
    public int numColumns() {
        return (firstResp == null || firstResp.value == null) ? 0 : firstResp.value.size();
    }

    @Override
    public String columnName(int column) {
        if (firstResp == null)
            return null;

        int pos_zero = 0;
        byte[] valData = firstResp.value.get(column).value;

        for (; pos_zero < valData.length && valData[pos_zero] > 0; ++pos_zero);

        return new String(valData, 0, pos_zero);
    }

    @Override
    public int columnType(int column) {
        return firstResp == null ? -1 : firstResp.value.get(column).type;
    }

    @Override
    public byte[] columnValue(int column) {
        return lastResp == null ? null : lastResp.value.get(column).value;
    }

    @Override
    public int rowsAffected() {
        while (next_int() == Errors.CDB2_OK)
            ;
        if (lastResp == null || lastResp.effects == null)
            return -1;

        return lastResp.effects.numAffected;
    }

    @Override
    public int rowsInserted() {
        while (next_int() == Errors.CDB2_OK)
            ;
        if (lastResp == null || lastResp.effects == null)
            return -1;

        return lastResp.effects.numInserted;
    }

    @Override
    public int rowsUpdated() {
        while (next_int() == Errors.CDB2_OK)
            ;
        if (lastResp == null || lastResp.effects == null)
            return -1;

        return lastResp.effects.numUpdated;
    }

    @Override
    public int rowsDeleted() {
        while (next_int() == Errors.CDB2_OK)
            ;
        if (lastResp == null || lastResp.effects == null)
            return -1;

        return lastResp.effects.numDeleted;
    }

    @Override
    public int rowsSelected() {
        while (next_int() == Errors.CDB2_OK)
            ;
        if (lastResp == null || lastResp.effects == null)
            return -1;

        return lastResp.effects.numSelected;
    }

    @Override
    public void clearParameters() {
        bindVars.clear();
        bindVarsByIndex.clear();
    }

    @Override
    public void bindParameter(int index, int type, byte[] data) {
        /* index is 1-based. */
        if (index <= 0)
            return;

        Cdb2BindValue newVal = new Cdb2BindValue();
        newVal.index = index;
        newVal.type = type;
        newVal.value = data;

        bindVarsByIndex.put(index, newVal);
    }

    @Override
    public void bindParameter(String name, int type, byte[] data) {

        // if the same name has been set multiple times, only the
        // lastest value will be kept and be sent to server.

        Cdb2BindValue newVal = new Cdb2BindValue();
        newVal.varName = name;
        newVal.type = type;
        newVal.value = data;

        bindVars.put(name, newVal);
    }

    @Override
    public void bindNamedParameters(Map<String, Cdb2Query.Cdb2BindValue> aBindVars) {
        bindVars.putAll(aBindVars);
    }

    @Override
    public void bindIndexedParameters(Map<Integer, Cdb2Query.Cdb2BindValue> aBindVars) {
        bindVarsByIndex.putAll(aBindVars);
    }

    @Override
    public String toString() {
        return String.format("[comdb2db] %d, %s. [comdb2_config] %s, %s, %d. [%s] %d, %s, %s",
                comdb2dbDbNum, comdb2dbHosts, defaultType, machineRoom,
                portMuxPort, myDbName, myDbNum, myDbHosts, myDbPorts);
    }

    /* Return the last IO error. */
    @Override
    public Throwable getLastThrowable() {
        return last_non_logical_err;
    }
}
/* vim: set sw=4 ts=4 et: */
