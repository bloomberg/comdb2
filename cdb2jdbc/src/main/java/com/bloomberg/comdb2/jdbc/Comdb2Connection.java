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
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.*;

import com.bloomberg.comdb2.jdbc.Constants.*;

/**
 * @author Rivers Zhang
 * @author Mohit Khullar
 * @author Tzvetan Mikov
 */
public class Comdb2Connection implements Connection {
    private static Logger logger = Logger.getLogger(Comdb2Connection.class.getName());

    public static final int TRANSACTION_SNAPSHOT = 128;
    public static final int TRANSACTION_SNAPSHOT_HA = 129;
    public static final int TRANSACTION_BLOCK = 130;

    /**
     * The dbhandle is shared amongst the connection, its statements and the
     * result sets of all its statements. Connection is responsible for
     * open/close the handle.
     */
    private Comdb2Handle hndl;
    /* this is to prevent connection pools from doing stupid things! 
       don't get it confused with Comdb2Handle.inTxn */
    private boolean inTxn = false;
    private boolean autoCommit = true;
    private boolean txnModeChanged = false;

    private Object lock = new Object();
    private boolean opened = false;

    private Comdb2DatabaseMetaData md = null;
    private String db, cluster;

    private int timeout = -1;
    private int querytimeout = -1;

    private String user;
    private String password;

    private boolean usemicrodt = true;

    /**
     * -1 indicates the default txn mode.
     */
    private int txnMode = -1;

    /**
     * Statements.
     */
    private ArrayList<Comdb2Statement> stmts = new ArrayList<Comdb2Statement>();

    public Comdb2Connection duplicate() {
        Comdb2Connection ret = new Comdb2Connection();
        ret.db = db;
        ret.cluster = cluster;
        ret.timeout = timeout;
        ret.querytimeout = querytimeout;
        ret.user = user;
        ret.password = password;
        ret.usemicrodt = usemicrodt;
        ret.hndl = hndl.duplicate();
        return ret;
    }

    /* Default constructor does not discover the database.
       This allows us to alter attributes of the connections
       and its underlying handle without discovering twice. */
    public Comdb2Connection() {
        hndl = new Comdb2Handle();
    }

    public Comdb2Handle dbHandle() {
        return this.hndl;
    }

    public Comdb2Connection(String db, String cluster) {
        /**
         * The handle is opened in the constructor.
         */
        this.db = db;
        this.cluster = cluster;
        hndl = new Comdb2Handle(db, cluster);
    }

    public void lookup() throws SQLException {
        try {
            hndl.lookup();
            opened = true;
        } catch (NoDbHostFoundException e) {
            throw new SQLException(e);
        }
    }

    /* Comdb2Statement needs these settings below */
    public void setQueryTimeout(int querytimeout) {
        this.querytimeout = querytimeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public void setSoTimeout(int timeout) {
        hndl.soTimeout = timeout;
    }

    public void setConnectTimeout(int timeout) {
        hndl.hasConnectTimeout = true;
        hndl.connectTimeout = timeout;
    }

    public void setComdb2dbTimeout(int timeout) {
        hndl.hasComdb2dbTimeout = true;
        hndl.comdb2dbTimeout = timeout;
    }

    public void setDbinfoTimeout(int timeout) {
        hndl.dbinfoTimeout = timeout;
    }

    public void setUser(String u) {
        user = u;
    }

    public void setPassword(String passwd) {
        password = passwd;
    }

    public void setMicroSecond(String v) {
        /* valid values: false, 0 or F */
        if ("false".equalsIgnoreCase(v)
                || "0".equalsIgnoreCase(v)
                || "F".equalsIgnoreCase(v))
            usemicrodt = false;
    }

    /* wrappers to Comdb2Handle */

    /* keep these 2 package visible */
    void addHosts(List<String> hosts) {
        hndl.addHosts(hosts);
    }

    void addPorts(List<Integer> ports) {
        hndl.addPorts(ports);
    }

    public void setDatabase(String db) {
        this.db = db;
        hndl.setDatabase(db);
    }

    public void setCluster(String cluster) {
        this.cluster = cluster;
        hndl.setCluster(cluster);
    }

    public void setPrefMach(String mach) {
        hndl.setPrefMach(mach);
    }

    public void setOverriddenPort(int port) {
        hndl.setOverriddenPort(port);
    }

    public void setDefaultType(String type) {
        hndl.setDefaultType(type);
    }

    public void setMachineRoom(String room) {
        hndl.setMachineRoom(room);
    }

    public void setPortMuxPort(int port) {
        hndl.setPortMuxPort(port);
    }

    public void setDnsSuffix(String suffix) {
        hndl.setDnsSuffix(suffix);
    }

    public void setComdb2dbName(String name) {
        hndl.setComdb2dbName(name);
    }

    public void setTcpBufSize(int sz) {
        hndl.setTcpBufSize(sz);
    }

    public void setPolicy(String policy) {
        hndl.setPolicy(policy);
    }

    public void setComdb2dbMaxAge(int age) {
        hndl.setComdb2dbMaxAge(age);
    }

    public void setMaxRetries(int n) {
        hndl.setMaxRetries(n);
    }

    public void setDebug(boolean dbg) {
        hndl.setDebug(dbg);
    }

    public void setSSLMode(String mode) {
        hndl.setSSLMode(mode);
    }

    public void setSSLCrt(String crt) {
        hndl.setSSLCrt(crt);
    }

    public void setSSLCrtPass(String crtpass) {
        hndl.setSSLCrtPass(crtpass);
    }

    public void setSSLCrtType(String crttype) {
        hndl.setSSLCrtType(crttype);
    }

    public void setSSLCA(String ca) {
        hndl.setSSLCA(ca);
    }

    public void setSSLCAPass(String capass) {
        hndl.setSSLCAPass(capass);
    }

    public void setSSLCAType(String catype) {
        hndl.setSSLCAType(catype);
    }

    public void setSSLCRL(String crl) {
        hndl.setSSLCRL(crl);
    }

    public void setAllowPmuxRoute(boolean rte) {
        hndl.setAllowPmuxRoute(rte);
    }

    public void setStatementQueryEffects(boolean stmtEffects) {
        hndl.setStatementQueryEffects(stmtEffects);
    }

    public void setVerifyRetry(boolean vrfyRetry) {
        hndl.setVerifyRetry(vrfyRetry);
    }

    public void setStackAtOpen(boolean sendStack) {
        hndl.hasSendStack = true;
        hndl.setSendStack(sendStack);
    }

    public ArrayList<String> getDbHosts() throws NoDbHostFoundException{
        return hndl.getDbHosts();
    }

    public ArrayList<Integer> getDbPorts() throws NoDbHostFoundException{
        return hndl.getDbPorts();
    }
    /* END OF wrappers to Comdb2Handle */

    public boolean isTxnModeChanged() {
        return txnModeChanged;
    }

    public void setTxnModeChanged(boolean txnModeChanged) {
        this.txnModeChanged = txnModeChanged;
    }

    public boolean isInTxn() {
        return inTxn;
    }

    public void setInTxn(boolean inTxn) {
        this.inTxn = inTxn;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    void removeStatement (Comdb2Statement stmt) {
        // TODO: pass an index to avoid the linear lookup, or use an IdentityHashMap
        stmts.remove(stmt);
    }

    @Override
    public Statement createStatement() throws SQLException {
        Comdb2Statement stmt = new Comdb2Statement(hndl, this);
        if (user != null)
            stmt.setUser(user);
        if (password != null)
            stmt.setPassword(password);
        if (timeout >= 0)
            stmt.setTimeout(timeout);
        if (querytimeout >= 0)
            stmt.setQueryTimeout(querytimeout);

        stmt.setUseMicroDt(usemicrodt);
        stmts.add(stmt);
        return stmt;
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        Comdb2PreparedStatement stmt = new Comdb2PreparedStatement(hndl, this,
                sql);
        if (user != null)
            stmt.setUser(user);
        if (password != null)
            stmt.setPassword(password);
        if (timeout >= 0)
            stmt.setTimeout(timeout);
        if (querytimeout >= 0)
            stmt.setQueryTimeout(querytimeout);

        stmt.setUseMicroDt(usemicrodt);
        stmts.add(stmt);
        return stmt;
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        return sql;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        this.autoCommit = autoCommit;
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return autoCommit;
    }

    @Override
    public void commit() throws SQLException {
        int rc;
        if (!autoCommit && inTxn) {
            inTxn = false;
            rc = hndl.runStatement("commit");
            if (rc != 0)
                throw Comdb2Connection.createSQLException(hndl.errorString(), rc, "commit",
                        hndl.getLastThrowable());
        }
    }

    @Override
    public void rollback() throws SQLException {
        int rc;
        if (!autoCommit && inTxn) {
            inTxn = false;
            rc = hndl.runStatement("rollback");
            if (rc != 0)
                throw Comdb2Connection.createSQLException(hndl.errorString(), rc, "rollback",
                        hndl.getLastThrowable());
        }
    }

    @Override
    public void close() throws SQLException {
        synchronized (lock) {
            if (opened == true) {

                for(Comdb2Statement stmt : stmts)
                    stmt.internalClose(false);

                stmts.clear(); // Let the GC do its magic

                if (hndl != null)
                    try {
                        hndl.close();
                    } catch (IOException e) {
                        logger.log(Level.WARNING,
                                   "Unable to close comdb2 connection to " + db + "/" + cluster,
                                   e);
                    }
                opened = false;
            }

            if (md != null) {
                md.conn.close();
                md = null;
            }
        }
    }

    @Override
    public boolean isClosed() throws SQLException {
        return !opened;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        if(md == null)
            md = new Comdb2DatabaseMetaData(duplicate());
        return md;
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        if (readOnly)
            hndl.runStatement("SET readonly");
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return false;
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public String getCatalog() throws SQLException {
        return db;
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        switch (level) {
        case Connection.TRANSACTION_SERIALIZABLE:
        case Connection.TRANSACTION_READ_COMMITTED:
        case Connection.TRANSACTION_REPEATABLE_READ:
        case TRANSACTION_SNAPSHOT:
        case TRANSACTION_SNAPSHOT_HA:
        case TRANSACTION_BLOCK:
            txnModeChanged = true;
            txnMode = level;
            break;
        case Connection.TRANSACTION_READ_UNCOMMITTED:
        default:
            txnMode = -1;
            break;
        }
    }

    public String getComdb2TxnMode() {
        switch (txnMode) {
        case Connection.TRANSACTION_SERIALIZABLE:
            return "serializable";
        case Connection.TRANSACTION_READ_COMMITTED:
            return "read committed";
        case Connection.TRANSACTION_REPEATABLE_READ:
            return "repeatable read";
        case TRANSACTION_SNAPSHOT:
            return "snapshot";
        case TRANSACTION_SNAPSHOT_HA:
            return "snapshot high availability";
        case TRANSACTION_BLOCK:
            return "blocksql";
        default:
            return null;
        }
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return txnMode;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType,
            int resultSetConcurrency) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getHoldability() throws SQLException {
        return java.sql.ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        rollback();
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Statement createStatement(int resultSetType,
            int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        return createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType,
            int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType,
            int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
            throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
            throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames)
            throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public Clob createClob() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Blob createBlob() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public NClob createNClob() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        /* driver will retry transparently in case of connection failure. so always return true */
        return true;
    }

    @Override
    public void setClientInfo(String name, String value)
            throws SQLClientInfoException {
    }

    @Override
    public void setClientInfo(Properties properties)
            throws SQLClientInfoException {
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements)
            throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes)
            throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void setSchema(String schema) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public String getSchema() throws SQLException {
        return cluster;
    }

    public void abort(Executor executor) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void setNetworkTimeout(Executor executor, int milliseconds)
            throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public int getNetworkTimeout() throws SQLException {
        return 0;
    }

    public String getDatabase() {
        return db;
    }

    public String getCluster() {
        return cluster;
    }

    public static SQLException createSQLException(String msg, int rc, String sql, Throwable ex) {
        /* references: */
        /* 1. SQL92 SPEC - http://www.contrib.andrew.cmu.edu/~shadow/sql/sql1992.txt */
        /* 2. SQL99 SPEC - https://mariadb.com/kb/en/sql-99/error-sqlstates/ */
        SQLException _ex;
        msg = "[RC = " + rc + "] " + msg;

        switch(rc) {
            /* connection - 08xxx */
            case Constants.Errors.CDB2ERR_CONNECT_ERROR:
                _ex = new SQLNonTransientConnectionException(msg, "08000", rc, ex);
                break;
            case Constants.Errors.CDB2ERR_NOTCONNECTED:
                _ex = new SQLNonTransientConnectionException(msg, "08003", rc, ex);
                break;
            case Constants.Errors.CDB2ERR_IO_ERROR:
                _ex = new SQLTransientConnectionException(msg, "08006", rc, ex);
                break;
            case Constants.Errors.CDB2ERR_REJECTED:
                _ex = new SQLTransientConnectionException(msg, "08004", rc, ex);
                break;
            case Constants.Errors.CDB2ERR_STOPPED:
                _ex = new SQLTransientConnectionException(msg, "08001", rc, ex);
                break;

            /* syntax and access rule - 42xxx */
            case Constants.Errors.CDB2ERR_PREPARE_ERROR:
            case Constants.Errors.CDB2ERR_FSQLPREPARE_ERROR:
            case Constants.Errors.CDB2ERR_BADCOLUMN:
            case Constants.Errors.CDB2ERR_NO_SUCH_DB:
            case Constants.Errors.CDB2ERR_NOSTATEMENT:
                _ex = new SQLSyntaxErrorException(msg, "42000", rc, ex);
                break;
            case Constants.Errors.CDB2ERR_READONLY:
                _ex = new SQLNonTransientException(msg, "42000", rc, ex);
                break;

            /* invalid credentials - 28xxx */
            case Constants.Errors.CDB2ERR_ACCESS:
                _ex = new SQLInvalidAuthorizationSpecException(msg, "28000", rc, ex);
                break;

            /* data exception - 22xxx */
            case Constants.Errors.CDB2ERR_BADREQ:
            case Constants.Errors.CDB2ERR_CONV_FAIL:
            case Constants.Errors.CDB2ERR_TZNAME_FAIL:
                _ex = new SQLNonTransientException(msg, "22000", rc, ex);
                break;

            /* invalid transaction state - 25xxx */
            case Constants.Errors.CDB2ERR_BADSTATE:
                _ex = new SQLNonTransientException(msg, "25000", rc, ex);
                break;

            /* feature not supported - 0Axxx */
            case Constants.Errors.CDB2ERR_TRAN_MODE_UNSUPPORTED:
                _ex = new SQLFeatureNotSupportedException(msg, "0A000", rc, ex);
                break;
            case Constants.Errors.CDB2ERR_NOTSUPPORTED:
                _ex = new SQLFeatureNotSupportedException(msg, "0A000", rc, ex);
                break;

            /* constraint violation - 23xxx */
            case Constants.Errors.CDB2ERR_CONSTRAINTS:
            case Constants.Errors.CDB2ERR_FKEY_VIOLATION:
            case Constants.Errors.CDB2ERR_NULL_CONSTRAINT:
            case Constants.Errors.CDB2ERR_DUPLICATE:
                _ex = new SQLIntegrityConstraintViolationException(msg, "23000", rc, ex);
                break;

            /* comdb2 errors */
            case Constants.Errors.CDB2ERR_INTERNAL:
            case Constants.Errors.CDB2ERR_DBCREATE_FAILED:
            case Constants.Errors.CDB2ERR_DBOP_FAILED:
            case Constants.Errors.CDB2ERR_THREADPOOL_INTERNAL:
            case Constants.Errors.CDB2ERR_ASYNCERR:
            case Constants.Errors.CDB2ERR_RECORD_OUT_OF_RANGE:
            case Constants.Errors.CDB2ERR_INVALID_ID:
            case Constants.Errors.CDB2ERR_NOMASTER:
            case Constants.Errors.CDB2ERR_NOTSERIAL:
            case Constants.Errors.CDB2ERR_CHANGENODE:
            case Constants.Errors.CDB2ERR_FASTSEED:
            case Constants.Errors.CDB2ERR_UNTAGGED_DATABASE:
            case Constants.Errors.CDB2ERR_DEADLOCK:
            case Constants.Errors.CDB2ERR_TRAN_IO_ERROR:
            case Constants.Errors.CDB2ERR_VERIFY_ERROR:
            case Constants.Errors.CDB2ERR_NONKLESS:
            case Constants.Errors.CDB2ERR_MALLOC:
            case Constants.Errors.CDB2ERR_SCHEMA:
            default:
                /* sql state "COMDB" catches all others */
                _ex = new SQLException(msg, "COMDB", rc, ex);
                break;
        }
        return _ex; 
    }

    public void clearAck() {
        hndl.clearAck();
    }
}
/* vim: set sw=4 ts=4 et: */
