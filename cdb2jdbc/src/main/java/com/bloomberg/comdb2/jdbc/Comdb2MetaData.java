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

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.bloomberg.comdb2.jdbc.Constants.Types;

/**
 * @author Rivers Zhang
 * @author Sebastien Blind
 */
public class Comdb2MetaData implements ResultSetMetaData {
    
    private DbHandle hndl;
    
    public Comdb2MetaData(DbHandle hndl) {
        this.hndl = hndl;
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
    public int getColumnCount() throws SQLException {
        return hndl.numColumns();
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        /**
         * For comdb2, always return false.
         */
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        return false;
    }

    @Override
    public int isNullable(int column) throws SQLException {
        return columnNullableUnknown;
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        return true;
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        return Integer.MAX_VALUE;
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return getColumnName(column);
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        return hndl.columnName(column - 1);
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return "";
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        return 0;
    }

    @Override
    public int getScale(int column) throws SQLException {
        return 0;
    }

    @Override
    public String getTableName(int column) throws SQLException {
        return "";
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return "";
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        --column; /* zero-based */
        switch (hndl.columnType(column)) {
            case Types.CDB2_INTEGER:
                return java.sql.Types.BIGINT;
            case Types.CDB2_REAL:
                return java.sql.Types.REAL;
            case Types.CDB2_CSTRING:
                return java.sql.Types.VARCHAR;
            case Types.CDB2_DATETIME:
            case Types.CDB2_DATETIMEUS:
                return java.sql.Types.TIMESTAMP;
            case Types.CDB2_BLOB:
                return java.sql.Types.BLOB;
            default:
                return java.sql.Types.OTHER;
        }
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        --column; /* zero-based */
        switch (hndl.columnType(column)) {
            case Types.CDB2_INTEGER:
                return "INTEGER";
            case Types.CDB2_REAL:
                return "REAL";
            case Types.CDB2_CSTRING:
                return "CSTRING";
            case Types.CDB2_DATETIME:
                return "DATETIME";
            case Types.CDB2_BLOB:
                return "BLOB";
            case Types.CDB2_INTERVALYM:
                return "INTERVALYM";
            case Types.CDB2_INTERVALDS:
                return "INTERVALDS";
            case Types.CDB2_DATETIMEUS:
                return "DATETIMEUS";
            case Types.CDB2_INTERVALDSUS:
                return "INTERVALDSUS";
            default:
                return "UNKNOWN";
        }
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        return "";
    }
}
/* vim: set sw=4 ts=4 et: */
