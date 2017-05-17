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

import java.io.Closeable;
import java.util.List;
import java.util.Map;

/**
 * DbHandle.
 * 
 * @author Rivers Zhang
 * @author Mohit Khullar
 */
public interface DbHandle extends Closeable {
    /**
     * Open the connection.
     * 
     * @return true if success, false otherwise.
     */
    boolean open();


    /**
     * Call it whenever database discovery is needed.
     */
    void lookup() throws NoDbHostFoundException;

    /**
     * Clear all bound parameters.
     */
    void clearParameters();

    /**
     * Bind a parameter.
     * 
     * @param name
     * @param type
     * @param data
     */
    void bindParameter(String name, int type, byte[] data);

    /**
     * Bind parameters.
     */
    void bindParameters(Map<String, Cdb2Query.Cdb2BindValue> bindVars);

    /**
     * Run @sql.
     * 
     * @param sql
     * @return 0 if success, non-zero otherwise.
     */
    int runStatement(String sql);

    /**
     * Run @sql.
     * 
     * @param sql
     * @throws Exception
     */
    void runStatementWithException(String sql);

    /**
     * Run typed @sql
     * 
     * @param sql
     * @param types
     * @return 0 if success, non-zero otherwise.
     */
    int runStatement(String sql, List<Integer> types);

    /**
     * Run typed @sql.
     * 
     * @param sql
     * @param types
     * @throws Exception
     */
    void runStatementWithException(String sql, List<Integer> types);

    /**
     * move cursor forward.
     * 
     * @return true if next record is available, false if no more.
     */
    boolean nextWithException();
    
    /**
     * Clear all pending results.
     */
    void clearResult();

    /**
     * move cursor forward.
     * 
     * @return 0 if more, 1 if no more.
     */
    int next();

    /**
     * Return the number of columns.
     * 
     * @return number of columns.
     */
    int numColumns();

    /**
     * Return the name of the @column th column.
     * 
     * @param column
     * @return column name.
     */
    String columnName(int column);

    /**
     * Return the type of the @column th column.
     * 
     * @param column
     * @return column type
     */
    int columnType(int column);

    /**
     * Return the value of the @column th column.
     * 
     * @param column
     * @return column value
     */
    byte[] columnValue(int column);

    /**
     * @return number of rows selected.
     */
    int rowsSelected();

    /**
     * @return number of rows affected.
     */
    int rowsAffected();

    /**
     * @return number of rows updated.
     */
    int rowsUpdated();

    /**
     * @return number of rows inserted.
     */
    int rowsInserted();

    /**
     * @return number of rows deleted.
     */
    int rowsDeleted();

    /**
     * @return error string.
     */
    String errorString();

    /*
     * Return the last Throwable.
     */
    Throwable getLastThrowable();
}
/* vim: set sw=4 ts=4 et: */
