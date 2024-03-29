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
import java.math.BigDecimal;
import java.net.URL;
import java.nio.ByteBuffer;
import java.sql.*;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;

import com.bloomberg.comdb2.jdbc.Cdb2Query.Cdb2BindValue;
import com.bloomberg.comdb2.jdbc.Cdb2Types.Datetime;
import com.bloomberg.comdb2.jdbc.Cdb2Types.DatetimeUs;

/**
 * @author Rivers Zhang
 * @author Mohit Khullar
 * @author Sebastien Blind
 * @author Tzvetan Mikov
 */
public class Comdb2PreparedStatement extends Comdb2Statement implements PreparedStatement {

    String sql;
    protected int[] types;

    /* my own bound paramters */
    protected HashMap<Integer, Cdb2BindValue> intBindVars = new HashMap<Integer, Cdb2BindValue>();
    protected List<HashMap<Integer, Cdb2BindValue>> batch;

    public Comdb2PreparedStatement(DbHandle hndl, Comdb2Connection conn, String sqlstr) {
        super(hndl, conn);
        ArrayList<String> names = new ArrayList<String>();
        sql = sqlstr;
    }

    public void bindParameter(int index, int type, byte[] data) {
        /* index is 1-based. */
        if (index <= 0)
            return;

        Cdb2BindValue newVal = new Cdb2BindValue();
        newVal.index = index;
        newVal.type = type;
        newVal.value = data;
        intBindVars.put(index, newVal);
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        // clear once in case of executeQuery() throws an exception.
        hndl.clearParameters();
        ResultSet ret;
        try {
            hndl.bindIndexedParameters(intBindVars);
            ret = executeQuery(sql);
        } finally {
            hndl.clearParameters();
        }
        return ret;
    }

    @Override
    public int executeUpdate() throws SQLException {
        // clear once in case of executeQuery() throws an exception.
        hndl.clearParameters();
        int ret;
        try {
            hndl.bindIndexedParameters(intBindVars);
            ret = executeUpdate(sql);
        } finally {
            hndl.clearParameters();
        }
        return ret;
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        bindParameter(parameterIndex, Constants.Types.CDB2_INTEGER, null);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        setLong(parameterIndex, x ? 1L : 0L);
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        setLong(parameterIndex, x);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        setLong(parameterIndex, x);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        setLong(parameterIndex, x);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        ByteBuffer bb = ByteBuffer.allocate(8);
        bb.putLong(x);
        bindParameter(parameterIndex, Constants.Types.CDB2_INTEGER, bb.array());
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        setDouble(parameterIndex, x);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        ByteBuffer bb = ByteBuffer.allocate(8);
        bb.putDouble(x);
        bindParameter(parameterIndex, Constants.Types.CDB2_REAL, bb.array());
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        setString(parameterIndex, x == null ? null : x.toPlainString());
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        byte[] data;
        if (x == null)
            data = null;
        else if (x.length() == 0)
            /* R6 and old R7 ignore the isnull flag for cstring and hence always
               treat a 0-length string as NULL. We have to send a zero byte here
               to be backward compatible. */
            data = new byte[1];
        else
            data = x.getBytes();
        bindParameter(parameterIndex, Constants.Types.CDB2_CSTRING, data);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        bindParameter(parameterIndex, Constants.Types.CDB2_BLOB, x);
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        if (x == null)
            bindParameter(parameterIndex, Constants.Types.CDB2_DATETIME, null);
        else if (!usemicrodt)
            bindParameter(parameterIndex, Constants.Types.CDB2_DATETIME,
                    Cdb2Types.Datetime.fromLong(x.getTime()).toBytes());
        else
            bindParameter(parameterIndex, Constants.Types.CDB2_DATETIMEUS,
                    Cdb2Types.DatetimeUs.fromLong(x.getTime() * 1000L).toBytes());
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        if (x == null)
            bindParameter(parameterIndex, Constants.Types.CDB2_DATETIME, null);
        else if (!usemicrodt)
            bindParameter(parameterIndex, Constants.Types.CDB2_DATETIME,
                    Cdb2Types.Datetime.fromLong(x.getTime()).toBytes());
        else
            bindParameter(parameterIndex, Constants.Types.CDB2_DATETIMEUS,
                    Cdb2Types.DatetimeUs.fromLong(x.getTime() * 1000L).toBytes());
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        if (x == null)
            bindParameter(parameterIndex, Constants.Types.CDB2_DATETIME, null);
        else if (!usemicrodt)
            bindParameter(parameterIndex, Constants.Types.CDB2_DATETIME,
                    Cdb2Types.Datetime.fromLong(x.getTime()).toBytes());
        else
            bindParameter(parameterIndex, Constants.Types.CDB2_DATETIMEUS,
                    Cdb2Types.DatetimeUs.fromLong(
                        x.getTime() / 1000L * 1000000L + x.getNanos() / 1000L
                        ).toBytes());
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        setUnicodeStream(parameterIndex, x, length);
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        if (x == null || length == 0)
            setString(parameterIndex, null);
        else {
            byte[] bytes = new byte[length];
            try {
                x.read(bytes);
                setString(parameterIndex, new String(bytes));
            } catch (IOException e) {
                SQLException exception = Comdb2Connection.createSQLException(
                        "failed to read stream", Constants.Errors.CDB2ERR_BADREQ, sql, e);
                throw exception;
            }
        }
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        if (x == null || length == 0)
            setBytes(parameterIndex, null);
        else {
            byte[] bytes = new byte[length];
            try {
                x.read(bytes);
                setBytes(parameterIndex, bytes);
            } catch (IOException e) {
                SQLException exception = Comdb2Connection.createSQLException(
                        "failed to read stream", Constants.Errors.CDB2ERR_BADREQ, sql, e);
                throw exception;
            }
        }
    }

    @Override
    public void clearParameters() throws SQLException {
        /* clear my own bound parameters- DO NOT touch hndl. */
        intBindVars.clear();
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        if (x == null) {
            setNull(parameterIndex, Types.NULL);
        } else if (x instanceof Cdb2Types.IntervalYearMonth) {
            Cdb2Types.IntervalYearMonth ym = (Cdb2Types.IntervalYearMonth) x;
            bindParameter(parameterIndex, Constants.Types.CDB2_INTERVALYM, ym.toBytes());
        } else if (x instanceof Cdb2Types.IntervalDaySecond) {
            Cdb2Types.IntervalDaySecond ym = (Cdb2Types.IntervalDaySecond) x;
            bindParameter(parameterIndex, Constants.Types.CDB2_INTERVALDS, ym.toBytes());
        } else if (x instanceof Cdb2Types.IntervalDaySecondUs) {
            Cdb2Types.IntervalDaySecondUs ym = (Cdb2Types.IntervalDaySecondUs) x;
            bindParameter(parameterIndex, Constants.Types.CDB2_INTERVALDSUS, ym.toBytes());
        } else if (x instanceof Cdb2Types.Datetime) {
            Cdb2Types.Datetime dt = (Cdb2Types.Datetime) x;
            bindParameter(parameterIndex, Constants.Types.CDB2_DATETIME, dt.toBytes());
        } else if (x instanceof Cdb2Types.DatetimeUs) {
            Cdb2Types.DatetimeUs dt = (Cdb2Types.DatetimeUs) x;
            bindParameter(parameterIndex, Constants.Types.CDB2_DATETIMEUS, dt.toBytes());
        } else if (x instanceof Cdb2Types.Blob) {
            Cdb2Types.Blob binary = (Cdb2Types.Blob) x;
            bindParameter(parameterIndex, Constants.Types.CDB2_BLOB, binary.toBytes());
        } else if (x instanceof Cdb2Types.CString) {
            Cdb2Types.CString str = (Cdb2Types.CString) x;
            bindParameter(parameterIndex, Constants.Types.CDB2_CSTRING, str.toBytes());
        } else if (x instanceof Cdb2Types.Real) {
            Cdb2Types.Real real = (Cdb2Types.Real) x;
            bindParameter(parameterIndex, Constants.Types.CDB2_REAL, real.toBytes());
        } else if (x instanceof Cdb2Types.Int64) {
            Cdb2Types.Int64 int64 = (Cdb2Types.Int64) x;
            bindParameter(parameterIndex, Constants.Types.CDB2_INTEGER, int64.toBytes());
        } else if (x instanceof java.util.Calendar) {
            Calendar cal = (Calendar) x;
            Datetime dt = new Datetime(
                    cal.get(Calendar.SECOND),
                    cal.get(Calendar.MINUTE),
                    cal.get(Calendar.HOUR_OF_DAY),
                    cal.get(Calendar.DAY_OF_MONTH),
                    cal.get(Calendar.MONTH),
                    cal.get(Calendar.YEAR) - 1900,
                    cal.get(Calendar.DAY_OF_WEEK),
                    cal.get(Calendar.DAY_OF_YEAR),
                    (cal.get(Calendar.DST_OFFSET) == 0) ? 0 : 1,
                    cal.get(Calendar.MILLISECOND),
                    cal.getTimeZone().getID()
                    );
            bindParameter(parameterIndex, Constants.Types.CDB2_DATETIME, dt.toBytes());
        } else if (x instanceof Array) {
            setArray(parameterIndex, (Array) x);
        } else if (x instanceof BigDecimal) {
            setBigDecimal(parameterIndex, (BigDecimal) x);
        } else if (x instanceof Byte) {
            setByte(parameterIndex, (Byte) x);
        } else if (x instanceof byte[]) {
            setBytes(parameterIndex, (byte[]) x);
        } else if (x instanceof Date) {
            setDate(parameterIndex, (Date) x);
        } else if (x instanceof Double) {
            setDouble(parameterIndex, (Double) x);
        } else if (x instanceof Float) {
            setFloat(parameterIndex, (Float) x);
        } else if (x instanceof Integer) {
            setInt(parameterIndex, (Integer) x);
        } else if (x instanceof Long) {
            setLong(parameterIndex, (Long) x);
        } else if (x instanceof Time) {
            setTime(parameterIndex, (Time) x);
        } else if (x instanceof Timestamp) {
            setTimestamp(parameterIndex, (Timestamp) x);
        } else if (x instanceof java.util.Date) {
            setDate(parameterIndex, new Date(((java.util.Date) x).getTime()));
        } else if (x instanceof Short) {
            setShort(parameterIndex, (Short) x);
        } else if (x instanceof String) {
            setString(parameterIndex, (String) x);
        } else if (x instanceof Blob) {
            setBlob(parameterIndex, (Blob) x);
        } else if (x instanceof NClob) {
            setNClob(parameterIndex, (NClob) x);
        } else if (x instanceof Clob) {
            setClob(parameterIndex, (Clob) x);
        } else {
            // Default to string representation for unknown types
            setString(parameterIndex, x.toString());
        }
    }

    @Override
    public boolean execute() throws SQLException {
        executeQuery();

        String lowerCase = sql.toLowerCase().trim();
        if (lowerCase.startsWith("delete") || lowerCase.startsWith("update") || lowerCase.startsWith("insert"))
            return false;
        return true;
    }

    @Override
    public void addBatch() throws SQLException {
        if (batch == null) {
            batch = new ArrayList<HashMap<Integer, Cdb2BindValue>>();
        }

        batch.add(intBindVars);
        intBindVars = new HashMap<Integer, Cdb2BindValue>();
    }

    @Override
    public void clearBatch() throws SQLException {
        if (batch != null)
            batch = null;
    }

    @Override
    public int[] executeBatch() throws SQLException {
        if (batch == null || batch.size() == 0) {
            return new int[0];
        }

        boolean autocommit = conn.getAutoCommit();
        boolean usetxn = conn.getUseTxnForBatch();

        if (usetxn) {
            /* Commit any open transactions. this will be a no-op if autocommit is true */
            conn.commit();
            conn.setAutoCommit(false);
        }

        int[] results = new int[batch.size()];

        for (int i = 0; i < batch.size(); i++) {
            intBindVars = batch.get(i);
            results[i] = executeUpdate();
        }

        if (usetxn) {
            /* commit this batch, and restore the autocommit-ness */
            conn.commit();
            conn.setAutoCommit(autocommit);
        }

        batch = null;
        return results;
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        if (reader == null || length == 0)
            setString(parameterIndex, null);
        else {
            char[] cbuf = new char[length];
            try {
                reader.read(cbuf);
                setString(parameterIndex, new String(cbuf));
            } catch (IOException e) {
                SQLException exception = Comdb2Connection.createSQLException(
                        "failed to read stream", Constants.Errors.CDB2ERR_BADREQ, sql, e);
                throw exception;
            }
        }
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        bindParameter(parameterIndex, Constants.Types.CDB2_BLOB,
                ((x == null) ? null : x.getBytes(1, (int) x.length())));
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        throw new SQLFeatureNotSupportedException("Please use ResultSet.getMetaData() instead");
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        setDate(parameterIndex, x);
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        setTime(parameterIndex, x);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        setTimestamp(parameterIndex, x);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        setNull(parameterIndex, sqlType);
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        setBinaryStream(parameterIndex, inputStream, length);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        setAsciiStream(parameterIndex, x, (int) length);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        setBinaryStream(parameterIndex, x, (int) length);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        setCharacterStream(parameterIndex, reader, (int) length);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        if (x == null)
            setString(parameterIndex, null);
        else {
            byte[] bytes = new byte[8192];
            byte[] dst = new byte[8192];
            int cnt, total = 0, pos = 0;

            try {
                while ((cnt = x.read(bytes)) >= 0) {
                    total += cnt;
                    if (total > dst.length) {
                        byte[] newdst = new byte[dst.length + 8192];
                        System.arraycopy(dst, 0, newdst, 0, pos);
                        System.arraycopy(bytes, 0, newdst, pos, cnt);

                        dst = null;
                        dst = newdst;
                    }
                    pos = total;
                }
                setString(parameterIndex, new String(dst, 0, total));
            } catch (IOException e) {
                throw Comdb2Connection.createSQLException(
                        "failed to read stream", Constants.Errors.CDB2ERR_BADREQ, sql, e);
            }
        }
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        if (x == null)
            setBytes(parameterIndex, null);
        else {
            byte[] bytes = new byte[8192];
            byte[] dst = new byte[8192];
            int cnt, total = 0, pos = 0;

            try {
                while ((cnt = x.read(bytes)) >= 0) {
                    total += cnt;
                    if (total > dst.length) {
                        byte[] newdst = new byte[dst.length + 8192];
                        System.arraycopy(dst, 0, newdst, 0, pos);
                        System.arraycopy(bytes, 0, newdst, pos, cnt);

                        dst = null;
                        dst = newdst;
                    }
                    pos = total;
                }

                byte[] finaldst = new byte[total];
                System.arraycopy(dst, 0, finaldst, 0, total);
                setBytes(parameterIndex, finaldst);
            } catch (IOException e) {
                throw Comdb2Connection.createSQLException(
                        "failed to read stream", Constants.Errors.CDB2ERR_BADREQ, sql, e);
            }
        }
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        if (reader == null)
            setString(parameterIndex, null);
        else {
            StringBuilder sb = new StringBuilder();
            char[] cbuf = new char[8192];
            int count;

            try {
                while ((count = reader.read(cbuf)) >= 0)
                    sb.append(cbuf, 0, count);

                setString(parameterIndex, sb.toString());
            } catch (IOException e) {
                throw Comdb2Connection.createSQLException(
                        "failed to read stream", Constants.Errors.CDB2ERR_BADREQ, sql, e);
            }
        }
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        setBinaryStream(parameterIndex, inputStream);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }
}
/* vim: set sw=4 ts=4 et: */
