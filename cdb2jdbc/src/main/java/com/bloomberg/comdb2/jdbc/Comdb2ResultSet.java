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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.charset.Charset;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Calendar;
import java.util.Map;
import java.util.TimeZone;

import javax.sql.rowset.serial.SerialBlob;

import com.bloomberg.comdb2.jdbc.Cdb2Types.*;
import com.bloomberg.comdb2.jdbc.Constants.Types;

/**
 * @author Rivers Zhang
 * @author Mohit Khullar
 * @author Sebastien Blind
 */
public class Comdb2ResultSet implements ResultSet {

    private DbHandle hndl;
    private ByteArray[] byteMatrix;
    private String[] columnNames;
    private SQLWarning warning;
    private boolean wasNull;
    private Comdb2Statement stmt;
    private String sql;

    public Comdb2ResultSet(DbHandle hndl, Comdb2Statement stmt, String sql) {
        this.hndl = hndl;
        this.stmt = stmt;
        this.sql  = sql;
        byteMatrix = new ByteArray[hndl.numColumns()];
        columnNames = new String[hndl.numColumns()];
    }

    public String getSql() {
        return sql;
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
    public boolean next() throws SQLException {
        int rc;
        for (int i = 0, len = byteMatrix.length; i != len; ++i)
            byteMatrix[i] = null;

        rc = hndl.next();
        if (rc == 0)
            return true;
        if (rc == 1)
            return false;
        throw Comdb2Connection.createSQLException(
                hndl.errorString(), rc, sql, hndl.getLastThrowable());
    }

    @Override
    public void close() throws SQLException {
        hndl.clearResult();
    }

    @Override
    public boolean wasNull() throws SQLException {
        return wasNull;
    }

    /**
     * Returns column index by the column name. A valid index shall be larger
     * than 0.
     * 
     * @param name
     * @return
     */
    private int getIndexByColumnName(String name) throws SQLException {

        int len = columnNames.length;

        if (len > 0 && columnNames[0] == null) {
            /**
             * if the name lookup table wasn't initialized, ask hndl to provide
             * names of all columns and fill out the lookup table.
             */

            for (int i = 0; i != len; ++i)
                columnNames[i] = hndl.columnName(i);
        }

        int idx = -1;
        for (int i = 0; i != len; ++i) {
            if (name.equalsIgnoreCase(columnNames[i])) {
                idx = i;
                break;
            }
        }

        if (idx < 0)
            throw Comdb2Connection.createSQLException(
                    "no such column `" + name + "'",
                    Constants.Errors.CDB2ERR_BADCOLUMN, sql, null);
        return idx + 1;
    }

    private ByteArray getByteArray(int columnIndex) {
        --columnIndex;
        ByteArray ba = null;

        if (byteMatrix[columnIndex] != null)
            ba = byteMatrix[columnIndex];
        else {

            byte[] bytes = hndl.columnValue(columnIndex);

            if (bytes == null || bytes.length == 0) {
                wasNull = true;
                return null;
            }

            wasNull = false;
            switch (hndl.columnType(columnIndex)) {
            case Types.CDB2_INTEGER:
                ba = new Int64(bytes);
                break;
            case Types.CDB2_REAL:
                ba = new Real(bytes);
                break;
            case Types.CDB2_CSTRING:
                ba = new CString(bytes);
                break;
            case Types.CDB2_DATETIME:
                ba = new Datetime(bytes);
                break;
            case Types.CDB2_DATETIMEUS:
                ba = new DatetimeUs(bytes);
                break;
            case Types.CDB2_INTERVALYM:
                ba = new IntervalYearMonth(bytes);
                break;
            case Types.CDB2_INTERVALDS:
                ba = new IntervalDaySecond(bytes);
                break;
            case Types.CDB2_INTERVALDSUS:
                ba = new IntervalDaySecondUs(bytes);
                break;
            case Types.CDB2_BLOB:
            default:
                ba = new Cdb2Types.Blob(bytes);
                break;
            }
            byteMatrix[columnIndex] = ba;
        }

        return ba;
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        ByteArray ba = getByteArray(columnIndex);
        try {
            return ba == null ? null : ba.toString();
        } catch (UnsupportedConversionException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        ByteArray ba = getByteArray(columnIndex);
        try {
            return ba == null ? false : ba.toBool();
        } catch (UnsupportedConversionException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        ByteArray ba = getByteArray(columnIndex);
        try {
            return (ba == null) ? 0 : (byte) ba.toLong();
        } catch (UnsupportedConversionException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        ByteArray ba = getByteArray(columnIndex);
        try {
            return (ba == null) ? 0 : (short) ba.toLong();
        } catch (UnsupportedConversionException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        ByteArray ba = getByteArray(columnIndex);
        try {
            return (ba == null) ? 0 : (int) ba.toLong();
        } catch (UnsupportedConversionException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        ByteArray ba = getByteArray(columnIndex);
        try {
            return (ba == null) ? 0 : ba.toLong();
        } catch (UnsupportedConversionException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        ByteArray ba = getByteArray(columnIndex);
        try {
            return (ba == null) ? 0 : (float) ba.toDouble();
        } catch (UnsupportedConversionException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        ByteArray ba = getByteArray(columnIndex);
        try {
            return (ba == null) ? 0 : ba.toDouble();
        } catch (UnsupportedConversionException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        ByteArray ba = getByteArray(columnIndex);
        if (ba == null)
            return null;

        try {
            String str = ba.toString();
            BigDecimal dec = new BigDecimal(str);
            dec.setScale(scale);
            return dec;
        } catch (UnsupportedConversionException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        ByteArray ba = getByteArray(columnIndex);
        try {
            return (ba == null) ? null : ba.toBytes();
        } catch (UnsupportedConversionException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        Calendar cal = getCalendar(columnIndex);
        return (cal == null) ? null : new Date(cal.getTimeInMillis());
    }

    private Calendar getCalendar(int columnIndex) {
        ByteArray ba = getByteArray(columnIndex);
        if (ba == null)
            return null;

        long ms;
        Calendar cal;
        if (hndl.columnType(columnIndex - 1) == Types.CDB2_DATETIME) {
            Datetime datetime = (Datetime) ba;
            TimeZone tz = TimeZone.getTimeZone(datetime.tzname);
            cal = Calendar.getInstance(tz);
            cal.set(datetime.tm_year + 1900,
                    datetime.tm_mon,
                    datetime.tm_mday,
                    datetime.tm_hour,
                    datetime.tm_min,
                    datetime.tm_sec);
            ms = cal.getTimeInMillis() / 1000L * 1000L + datetime.msec;
        } else {
            DatetimeUs datetime = (DatetimeUs) ba;
            TimeZone tz = TimeZone.getTimeZone(datetime.tzname);
            cal = Calendar.getInstance(tz);
            cal.set(datetime.tm_year + 1900,
                    datetime.tm_mon,
                    datetime.tm_mday,
                    datetime.tm_hour,
                    datetime.tm_min,
                    datetime.tm_sec);
            ms = cal.getTimeInMillis() / 1000L * 1000L + datetime.usec / 1000L;
        }
        cal.setTimeInMillis(ms);
        return cal;
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        Calendar cal = getCalendar(columnIndex);
        return (cal == null) ? null : new Time(cal.getTimeInMillis());
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        ByteArray ba = getByteArray(columnIndex);
        if (ba == null)
            return null;

        Timestamp ret;
        if (hndl.columnType(columnIndex - 1) == Types.CDB2_DATETIME) {
            Datetime datetime = (Datetime) ba;
            TimeZone tz = TimeZone.getTimeZone(datetime.tzname);
            Calendar cal = Calendar.getInstance(tz);
            cal.set(datetime.tm_year + 1900,
                    datetime.tm_mon,
                    datetime.tm_mday,
                    datetime.tm_hour,
                    datetime.tm_min,
                    datetime.tm_sec);
            long ms = cal.getTimeInMillis() / 1000L * 1000L + datetime.msec;
            ret = new Timestamp(ms);
        } else {
            DatetimeUs datetime = (DatetimeUs) ba;
            TimeZone tz = TimeZone.getTimeZone(datetime.tzname);
            Calendar cal = Calendar.getInstance(tz);
            cal.set(datetime.tm_year + 1900,
                    datetime.tm_mon,
                    datetime.tm_mday,
                    datetime.tm_hour,
                    datetime.tm_min,
                    datetime.tm_sec);
            long ms = cal.getTimeInMillis() / 1000L * 1000L;
            ret = new Timestamp(ms);
            ret.setNanos((int)(datetime.usec * 1000L));
        }
        return ret;
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        byte[] barr = getBytes(columnIndex);
        if (barr == null)
            return null;
        return new ByteArrayInputStream(Charset.forName("ISO-Latin-1").encode(new String(barr)).array());
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        byte[] barr = getBytes(columnIndex);
        if (barr == null)
            return null;
        return new ByteArrayInputStream(Charset.forName("UTF-16").encode(new String(barr)).array());
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        return new ByteArrayInputStream(getBytes(columnIndex));
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return getString(getIndexByColumnName(columnLabel));
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return getBoolean(getIndexByColumnName(columnLabel));
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return getByte(getIndexByColumnName(columnLabel));
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return getShort(getIndexByColumnName(columnLabel));
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return getInt(getIndexByColumnName(columnLabel));
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return getLong(getIndexByColumnName(columnLabel));
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return getFloat(getIndexByColumnName(columnLabel));
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return getDouble(getIndexByColumnName(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return getBigDecimal(getIndexByColumnName(columnLabel), scale);
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return getBytes(getIndexByColumnName(columnLabel));
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return getDate(getIndexByColumnName(columnLabel));
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return getTime(getIndexByColumnName(columnLabel));
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getTimestamp(getIndexByColumnName(columnLabel));
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        return getAsciiStream(getIndexByColumnName(columnLabel));
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        return getUnicodeStream(getIndexByColumnName(columnLabel));
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        return getBinaryStream(getIndexByColumnName(columnLabel));
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return warning;
    }

    @Override
    public void clearWarnings() throws SQLException {
        warning = null;
    }

    @Override
    public String getCursorName() throws SQLException {
        return null;
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return new Comdb2MetaData(hndl);
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        ByteArray byteArray = getByteArray(columnIndex);

        if (byteArray instanceof Cdb2Types.CString) {
            return getString(columnIndex);
        } else if (byteArray instanceof Cdb2Types.Datetime) {
            return getTimestamp(columnIndex);
        } else if (byteArray instanceof Cdb2Types.DatetimeUs) {
            return getTimestamp(columnIndex);
        } else if (byteArray instanceof Cdb2Types.Int64) {
            return getLong(columnIndex);
        } else if (byteArray instanceof Cdb2Types.Real) {
            return getDouble(columnIndex);
        } else {
            return byteArray;
        }
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getObject(getIndexByColumnName(columnLabel));
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        return getIndexByColumnName(columnLabel);
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        return new InputStreamReader(getBinaryStream(columnIndex));
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        return getCharacterStream(getIndexByColumnName(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        ByteArray ba = getByteArray(columnIndex);
        try {
            return (ba == null) ? null : new BigDecimal(ba.toString());
        } catch (UnsupportedConversionException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return getBigDecimal(getIndexByColumnName(columnLabel));
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        return ((Comdb2Handle)hndl).isBeforeFirst;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        return ((Comdb2Handle)hndl).isAfterLast;
    }

    @Override
    public boolean isFirst() throws SQLException {
        return ((Comdb2Handle)hndl).isFirst;
    }

    @Override
    public boolean isLast() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void beforeFirst() throws SQLException {
        throw new SQLException("The method should not be called on a TYPE_FORWARD_ONLY resultset");
    }

    @Override
    public void afterLast() throws SQLException {
        throw new SQLException("The method should not be called on a TYPE_FORWARD_ONLY resultset");
    }

    @Override
    public boolean first() throws SQLException {
        throw new SQLException("The method should not be called on a TYPE_FORWARD_ONLY resultset");
    }

    @Override
    public boolean last() throws SQLException {
        throw new SQLException("The method should not be called on a TYPE_FORWARD_ONLY resultset");
    }

    @Override
    public int getRow() throws SQLException {
        return 0;
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        throw new SQLException("The method should not be called on a TYPE_FORWARD_ONLY resultset");
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        throw new SQLException("The method should not be called on a TYPE_FORWARD_ONLY resultset");
    }

    @Override
    public boolean previous() throws SQLException {
        throw new SQLException("The method should not be called on a TYPE_FORWARD_ONLY resultset");
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        if (direction != ResultSet.FETCH_FORWARD)
            throw new SQLException("The fetch direction of a TYPE_FORWARD_ONLY resultset can only be set to FETCH_FORWARD");
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getFetchSize() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getType() throws SQLException {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public int getConcurrency() throws SQLException {
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean rowInserted() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void insertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void deleteRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void refreshRow() throws SQLException {
        throw new SQLException("The method should not be called on a TYPE_FORWARD_ONLY resultset");
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Statement getStatement() throws SQLException {
        return stmt;
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        ByteArray ba = getByteArray(columnIndex);
        try {
            return (ba == null) ? null : new SerialBlob(ba.toBytes());
        } catch (UnsupportedConversionException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        return getBlob(getIndexByColumnName(columnLabel));
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        return getDate(columnIndex);
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return getDate(columnLabel);
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        return getTime(columnIndex);
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return getTime(columnLabel);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        return getTimestamp(columnIndex);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return getTimestamp(columnLabel);
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getHoldability() throws SQLException {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return false;
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        return (T) getByteArray(columnIndex);
    }

    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return (T) getByteArray(getIndexByColumnName(columnLabel));
    }

}
/* vim: set sw=4 ts=4 et: */
