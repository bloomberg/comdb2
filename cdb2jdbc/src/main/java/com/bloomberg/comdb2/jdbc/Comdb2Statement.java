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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.*;

/**
 * @author Rivers Zhang
 * @author Mohit Khullar
 * @author Sebastien Blind
 * @author Tzvetan Mikov
 */
public class Comdb2Statement implements Statement {
    private static Logger logger = Logger.getLogger(Comdb2Statement.class.getName());

    /**
     * `hndl` is a reference to the handle of its connection. So it should be
     *       freed by the statement.
     */
    protected DbHandle hndl;
    protected Comdb2Connection conn;
    protected Comdb2ResultSet rs;
    protected int timeout = -1;
    protected int querytimeout = -1;
    protected boolean closed;
    protected String user;
    protected String password;
    protected boolean usemicrodt = true;

    public Comdb2Statement(DbHandle hndl, Comdb2Connection conn) {
        this(hndl, conn, -1, -1);
    }

    public Comdb2Statement(DbHandle hndl, Comdb2Connection conn, int timeout, int querytime) {
        this.hndl = hndl;
        this.conn = conn;
        this.timeout = timeout;
        this.querytimeout = querytime;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        return executeQuery(sql, null);
    }

    public ResultSet executeQuery(String sql, List<Integer> types) throws SQLException {
        int rc;
        String locase = sql.toLowerCase().trim();

        if(rs != null) {
            rs.close();
            rs = null;
        }

        if (!conn.isInTxn()) {
            if (querytimeout >= 0) {
                if ( (rc = hndl.runStatement("set maxquerytime " + querytimeout)) != 0 )
                    throw Comdb2Connection.createSQLException(
                            hndl.errorString(), rc, sql, hndl.getLastThrowable());
            }
            if (timeout >= 0) {
                if ( (rc = hndl.runStatement("set timeout " + timeout)) != 0 )
                    throw Comdb2Connection.createSQLException(
                            hndl.errorString(), rc, sql, hndl.getLastThrowable());
            }

            if (user != null && user.trim().length() > 0) {
                if ( (rc = hndl.runStatement("set user " + user)) != 0 )
                    throw Comdb2Connection.createSQLException(
                            hndl.errorString(), rc, sql, hndl.getLastThrowable());
            }

            if (password != null && password.trim().length() > 0) {
                if ( (rc = hndl.runStatement("set password " + password)) != 0 )
                    throw Comdb2Connection.createSQLException(
                            hndl.errorString(), rc, sql, hndl.getLastThrowable());
            }
        }

        if (!conn.getAutoCommit() && !conn.isInTxn()) {
            if (conn.isTxnModeChanged()) {
                String txnMode = conn.getComdb2TxnMode();
                if (txnMode != null) {
                    /**
                     * set transaction mode implicitly
                     */
                    if ( (rc = hndl.runStatement("set transaction " + txnMode)) != 0 )
                        throw Comdb2Connection.createSQLException(
                                hndl.errorString(), rc, sql, hndl.getLastThrowable());
                }
            }
            /**
             * send `begin' implicitly
             */
            if (!locase.startsWith("set ")) { /* just a set. don't begin yet. */
                if ( (rc = hndl.runStatement("begin")) != 0)
                    throw Comdb2Connection.createSQLException(
                            hndl.errorString(), rc, sql, hndl.getLastThrowable());
                /* mark connection in trans */
                conn.setInTxn(true);
            }
        }

        List<Integer> cdb2types = null;
        if (types != null) {
            cdb2types = new ArrayList<Integer>();
            for (Integer elem : types)
                cdb2types.add(Comdb2MetaData.sqlToComdb2(elem));
        }

        if ( (rc = hndl.runStatement(sql, cdb2types)) != 0 )
            throw Comdb2Connection.createSQLException(
                    hndl.errorString(), rc, sql, hndl.getLastThrowable());

        rs = new Comdb2ResultSet(hndl, this, sql);
        return rs;
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        String lowerCase = sql.toLowerCase().trim();

        if (!lowerCase.startsWith("delete") &&
            !lowerCase.startsWith("update") &&
            !lowerCase.startsWith("insert") &&
            /** ^^^^^^ DML
                            DDL vvvvvv  **/
            !lowerCase.startsWith("create") &&
            !lowerCase.startsWith("alter") &&
            !lowerCase.startsWith("drop") &&
            !lowerCase.startsWith("truncate"))
            throw Comdb2Connection.createSQLException(
                    "The SQL statement can only be an INSERT, UPDATE, DELETE or DDL.",
                    Constants.Errors.CDB2ERR_PREPARE_ERROR, sql, hndl.getLastThrowable());

        ResultSet rs = executeQuery(sql);
        int count = ((Comdb2Handle)hndl).rowsAffected();
        return count;
    }

    @Override
    public void close() throws SQLException {
        internalClose(true);
    }

    void internalClose (boolean removeSelf) throws SQLException {
        if (!closed) {
            closed = true;
            if (rs != null)
                rs.close();
            if (removeSelf)
                conn.removeStatement(this);
        }
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getMaxRows() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return querytimeout;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        this.querytimeout = seconds;
    }

    public void setTimeout(int milliseconds) throws SQLException {
        this.timeout = milliseconds;
    }

    @Override
    public void cancel() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        executeQuery(sql);

        String lowerCase = sql.toLowerCase().trim();
        if (lowerCase.startsWith("delete") || lowerCase.startsWith("update")
                || lowerCase.startsWith("insert"))
            return false;
        return true;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return rs;
    }

    @Override
    public int getUpdateCount() throws SQLException {

        if (rs == null)
            return -1;

        String lowerCase = rs.getSql().trim().toLowerCase();
        if (lowerCase.startsWith("delete") || lowerCase.startsWith("update")
                || lowerCase.startsWith("insert")) {
            int count = -1;
            while (rs.next())
                count = rs.getInt(1);
            return count;
        } else {
            /* not a write, return -1. */
            return -1;
        }
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return false;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
    }

    @Override
    public int getFetchSize() throws SQLException {
        return 0;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public int getResultSetType() throws SQLException {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void clearBatch() throws SQLException {
    }

    @Override
    public int[] executeBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Connection getConnection() throws SQLException {
        return conn;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return false;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys)
            throws SQLException {
        return executeUpdate(sql);
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes)
            throws SQLException {
        return executeUpdate(sql);
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames)
            throws SQLException {
        return executeUpdate(sql);
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys)
            throws SQLException {
        return execute(sql);
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return execute(sql);
    }

    @Override
    public boolean execute(String sql, String[] columnNames)
            throws SQLException {
        return execute(sql);
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return false;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isPoolable() throws SQLException {
        return false;
    }

    public void closeOnCompletion() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean isCloseOnCompletion() throws SQLException {
        return false;
    }

    public void setUser(String u) {
        user = u;
    }

    public void setPassword(String passwd) {
        password = passwd;
    }

    public void setUseMicroDt(boolean use) {
        usemicrodt = use;
    }
}
/* vim: set sw=4 ts=4 et: */
