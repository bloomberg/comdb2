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

import java.sql.*;
import java.util.*;

/**
 * Comdb2 database metadata.
 * @author Rivers Zhang
 * @author Sean Winard
 */
public class Comdb2DatabaseMetaData implements DatabaseMetaData {

    private Statement stmt;
    private PreparedStatement ps = null;
    private String url;
    Comdb2Connection conn;
    private String sqlkws = null;

    Comdb2DatabaseMetaData(Comdb2Connection conn) throws SQLException {
        url = "jdbc:comdb2:" + conn.getDatabase() + ":" + conn.getCluster();
        this.conn = conn;
        conn.lookup();
        stmt = conn.createStatement();
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    public boolean allProceduresAreCallable() throws SQLException {
        return true;
    }

    public boolean allTablesAreSelectable() throws SQLException {
        return true;
    }

    public String getURL() throws SQLException {
        return url;
    }

    public String getUserName() throws SQLException {
        return "";
    }

    public boolean isReadOnly() throws SQLException {
        return false;
    }

    public boolean nullsAreSortedHigh() throws SQLException {
        return true;
    }

    public boolean nullsAreSortedLow() throws SQLException {
        return false;
    }

    public boolean nullsAreSortedAtStart() throws SQLException {
        return true;
    }

    public boolean nullsAreSortedAtEnd() throws SQLException {
        return false;
    }

    public String getDatabaseProductName() throws SQLException {
        return "Comdb2";
    }

    public String getDatabaseProductVersion() throws SQLException {
        Comdb2Connection versionConn = conn.duplicate();
        try {
            /* Use a dedicated handle so metadata version reads do not invalidate
             * active result sets returned by getTables()/other prepared metadata calls.
             */
            versionConn.lookup();
            try (Statement versionStmt = versionConn.createStatement();
                    ResultSet rs = versionStmt.executeQuery("select comdb2_version()")) {
                String ret = "UNKNOWN";
                while (rs.next())
                    ret = rs.getString(1);
                return ret;
            }
        } finally {
            versionConn.close();
        }
    }

    public String getDriverName() throws SQLException {
        return Comdb2ClientInfo.getDriverName();
    }

    public String getDriverVersion() throws SQLException {
        return Comdb2ClientInfo.getDriverVersion();
    }

    public int getDriverMajorVersion() {
        return Comdb2ClientInfo.getDriverMajorVersion();
    }

    public int getDriverMinorVersion() {
        return Comdb2ClientInfo.getDriverMinorVersion();
    }

    public boolean usesLocalFiles() throws SQLException {
        return false;
    }

    public boolean usesLocalFilePerTable() throws SQLException {
        return false;
    }

    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        return true;
    }

    public boolean storesUpperCaseIdentifiers() throws SQLException {
        return false;
    }

    public boolean storesLowerCaseIdentifiers() throws SQLException {
        return false;
    }

    public boolean storesMixedCaseIdentifiers() throws SQLException {
        return true;
    }

    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    public String getIdentifierQuoteString() throws SQLException {
        return " ";
    }

    public String getSQLKeywords() throws SQLException {
        if (sqlkws != null)
            return sqlkws;

        int ii, len;
        StringBuilder sb = new StringBuilder();
        ArrayList<String> kws = new ArrayList<String>();

        HashSet<String> ht = new HashSet<String>(1024);

        /* http://savage.net.au/SQL/sql-2003-2.bnf.html#xref-keywords */
        ht.add("ABS");
        ht.add("ABSOLUTE");
        ht.add("ACTION");
        ht.add("ADA");
        ht.add("ADD");
        ht.add("ADMIN");
        ht.add("AFTER");
        ht.add("ALL");
        ht.add("ALLOCATE");
        ht.add("ALTER");
        ht.add("ALWAYS");
        ht.add("AND");
        ht.add("ANY");
        ht.add("ARE");
        ht.add("ARRAY");
        ht.add("AS");
        ht.add("ASC");
        ht.add("ASENSITIVE");
        ht.add("ASSERTION");
        ht.add("ASSIGNMENT");
        ht.add("ASYMMETRIC");
        ht.add("AT");
        ht.add("ATOMIC");
        ht.add("ATTRIBUTE");
        ht.add("ATTRIBUTES");
        ht.add("AUTHORIZATION");
        ht.add("auto");
        ht.add("AVG");
        ht.add("BEFORE");
        ht.add("BEGIN");
        ht.add("BERNOULLI");
        ht.add("BETWEEN");
        ht.add("BIGINT");
        ht.add("BIN");
        ht.add("BINARY");
        ht.add("BLOB");
        ht.add("BOOLEAN");
        ht.add("BOTH");
        ht.add("BREADTH");
        ht.add("BY");
        ht.add("C");
        ht.add("CALL");
        ht.add("CALLED");
        ht.add("CARDINALITY");
        ht.add("CASCADE");
        ht.add("CASCADED");
        ht.add("CASE");
        ht.add("CAST");
        ht.add("CATALOG");
        ht.add("CATALOG_NAME");
        ht.add("CEIL");
        ht.add("CEILING");
        ht.add("CHAIN");
        ht.add("char");
        ht.add("CHAR");
        ht.add("CHARACTER");
        ht.add("CHARACTERISTICS");
        ht.add("CHARACTERS");
        ht.add("CHARACTER_LENGTH");
        ht.add("CHARACTER_SET_CATALOG");
        ht.add("CHARACTER_SET_NAME");
        ht.add("CHARACTER_SET_SCHEMA");
        ht.add("CHAR_LENGTH");
        ht.add("CHECK");
        ht.add("CHECKED");
        ht.add("CLASS_ORIGIN");
        ht.add("CLOB");
        ht.add("CLOSE");
        ht.add("COALESCE");
        ht.add("COBOL");
        ht.add("CODE_UNITS");
        ht.add("COLLATE");
        ht.add("COLLATION");
        ht.add("COLLATION_CATALOG");
        ht.add("COLLATION_NAME");
        ht.add("COLLATION_SCHEMA");
        ht.add("COLLECT");
        ht.add("COLUMN");
        ht.add("COLUMN_NAME");
        ht.add("COMMAND_FUNCTION");
        ht.add("COMMAND_FUNCTION_CODE");
        ht.add("COMMIT");
        ht.add("COMMITTED");
        ht.add("CONDITION");
        ht.add("CONDITION_NUMBER");
        ht.add("CONNECT");
        ht.add("CONNECTION");
        ht.add("CONNECTION_NAME");
        ht.add("const");
        ht.add("CONSTRAINT");
        ht.add("CONSTRAINTS");
        ht.add("CONSTRAINT_CATALOG");
        ht.add("CONSTRAINT_NAME");
        ht.add("CONSTRAINT_SCHEMA");
        ht.add("CONSTRUCTOR");
        ht.add("CONSTRUCTORS");
        ht.add("CONTAINS");
        ht.add("CONTINUE");
        ht.add("CONVERT");
        ht.add("CORR");
        ht.add("CORRESPONDING");
        ht.add("COUNT");
        ht.add("COVAR_POP");
        ht.add("COVAR_SAMP");
        ht.add("CREATE");
        ht.add("CROSS");
        ht.add("CUBE");
        ht.add("CUME_DIST");
        ht.add("CURRENT");
        ht.add("CURRENT_COLLATION");
        ht.add("CURRENT_DATE");
        ht.add("CURRENT_DEFAULT_TRANSFORM_GROUP");
        ht.add("CURRENT_PATH");
        ht.add("CURRENT_ROLE");
        ht.add("CURRENT_TIME");
        ht.add("CURRENT_TIMESTAMP");
        ht.add("CURRENT_TRANSFORM_GROUP_FOR_TYPE");
        ht.add("CURRENT_USER");
        ht.add("CURSOR");
        ht.add("CURSOR_NAME");
        ht.add("CYCLE");
        ht.add("DATA");
        ht.add("DATE");
        ht.add("DATETIME_INTERVAL_CODE");
        ht.add("DATETIME_INTERVAL_PRECISION");
        ht.add("DAY");
        ht.add("DCL");
        ht.add("DEALLOCATE");
        ht.add("DEC");
        ht.add("DECIMAL");
        ht.add("DECLARE");
        ht.add("DEFAULT");
        ht.add("DEFAULTS");
        ht.add("DEFERRABLE");
        ht.add("DEFERRED");
        ht.add("DEFINED");
        ht.add("DEFINER");
        ht.add("DEGREE");
        ht.add("DELETE");
        ht.add("DENSE_RANK");
        ht.add("DEPTH");
        ht.add("DEREF");
        ht.add("DERIVED");
        ht.add("DESC");
        ht.add("DESCRIBE");
        ht.add("DESCRIPTOR");
        ht.add("DETERMINISTIC");
        ht.add("DIAGNOSTICS");
        ht.add("DISCONNECT");
        ht.add("DISPATCH");
        ht.add("DISPLAY");
        ht.add("DISTINCT");
        ht.add("DOMAIN");
        ht.add("double");
        ht.add("DOUBLE");
        ht.add("DOUBLE_PRECISION");
        ht.add("DROP");
        ht.add("DYNAMIC");
        ht.add("DYNAMIC_FUNCTION");
        ht.add("DYNAMIC_FUNCTION_CODE");
        ht.add("EACH");
        ht.add("ELEMENT");
        ht.add("ELSE");
        ht.add("END");
        ht.add("END-EXEC");
        ht.add("EQUALS");
        ht.add("ESCAPE");
        ht.add("EVERY");
        ht.add("EXCEPT");
        ht.add("EXCEPTION");
        ht.add("EXCLUDE");
        ht.add("EXCLUDING");
        ht.add("EXEC");
        ht.add("EXECUTE");
        ht.add("EXISTS");
        ht.add("EXP");
        ht.add("extern");
        ht.add("EXTERNAL");
        ht.add("EXTRACT");
        ht.add("FALSE");
        ht.add("FETCH");
        ht.add("FILTER");
        ht.add("FINAL");
        ht.add("FIRST");
        ht.add("FIXED");
        ht.add("FLOAT");
        ht.add("float");
        ht.add("FLOOR");
        ht.add("FOLLOWING");
        ht.add("FOR");
        ht.add("FOREIGN");
        ht.add("FORTRAN");
        ht.add("FOUND");
        ht.add("FREE");
        ht.add("FROM");
        ht.add("FULL");
        ht.add("FUNCTION");
        ht.add("FUSION");
        ht.add("GENERAL");
        ht.add("GENERATED");
        ht.add("GET");
        ht.add("GLOBAL");
        ht.add("GO");
        ht.add("GOTO");
        ht.add("GRANT");
        ht.add("GRANTED");
        ht.add("GROUP");
        ht.add("GROUPING");
        ht.add("HAVING");
        ht.add("HIERARCHY");
        ht.add("HOLD");
        ht.add("HOUR");
        ht.add("IDENTITY");
        ht.add("IMMEDIATE");
        ht.add("IMPLEMENTATION");
        ht.add("IN");
        ht.add("INCLUDING");
        ht.add("INCREMENT");
        ht.add("INDICATOR");
        ht.add("INDICATOR_TYPE");
        ht.add("INITIALLY");
        ht.add("INNER");
        ht.add("INOUT");
        ht.add("INPUT");
        ht.add("INSENSITIVE");
        ht.add("INSERT");
        ht.add("INSTANCE");
        ht.add("INSTANTIABLE");
        ht.add("INT");
        ht.add("INTEGER");
        ht.add("Interfaces");
        ht.add("INTERSECT");
        ht.add("INTERSECTION");
        ht.add("INTERVAL");
        ht.add("INTO");
        ht.add("INVOKER");
        ht.add("IS");
        ht.add("ISOLATION");
        ht.add("JOIN");
        ht.add("KEY");
        ht.add("KEY_MEMBER");
        ht.add("KEY_TYPE");
        ht.add("KIND");
        ht.add("LANGUAGE");
        ht.add("LARGE");
        ht.add("LAST");
        ht.add("LATERAL");
        ht.add("LEADING");
        ht.add("LEFT");
        ht.add("LENGTH");
        ht.add("LEVEL");
        ht.add("LIKE");
        ht.add("LN");
        ht.add("LOCAL");
        ht.add("LOCALTIME");
        ht.add("LOCALTIMESTAMP");
        ht.add("LOCATOR");
        ht.add("LOGICAL");
        ht.add("long");
        ht.add("LOWER");
        ht.add("MAP");
        ht.add("MATCH");
        ht.add("MATCHED");
        ht.add("MAX");
        ht.add("MAXVALUE");
        ht.add("MEMBER");
        ht.add("MERGE");
        ht.add("MESSAGE_LENGTH");
        ht.add("MESSAGE_OCTET_LENGTH");
        ht.add("MESSAGE_TEXT");
        ht.add("METHOD");
        ht.add("MIN");
        ht.add("MINUTE");
        ht.add("MINVALUE");
        ht.add("MOD");
        ht.add("MODIFIES");
        ht.add("MODULE");
        ht.add("MONTH");
        ht.add("MORE");
        ht.add("MULTISET");
        ht.add("MUMPS");
        ht.add("NAME");
        ht.add("NAMES");
        ht.add("NATIONAL");
        ht.add("NATURAL");
        ht.add("NCHAR");
        ht.add("NCLOB");
        ht.add("NESTING");
        ht.add("NEW");
        ht.add("NEXT");
        ht.add("NO");
        ht.add("NONE");
        ht.add("NORMALIZE");
        ht.add("NORMALIZED");
        ht.add("NOT");
        ht.add("NULL");
        ht.add("NULLABLE");
        ht.add("NULLIF");
        ht.add("NULLS");
        ht.add("NUMBER");
        ht.add("NUMERIC");
        ht.add("OBJECT");
        ht.add("OCTETS");
        ht.add("OCTET_LENGTH");
        ht.add("OF");
        ht.add("OLD");
        ht.add("ON");
        ht.add("ONLY");
        ht.add("OPEN");
        ht.add("OPTION");
        ht.add("OPTIONS");
        ht.add("OR");
        ht.add("ORDER");
        ht.add("ORDERING");
        ht.add("ORDINALITY");
        ht.add("OTHERS");
        ht.add("OUT");
        ht.add("OUTER");
        ht.add("OUTPUT");
        ht.add("OVER");
        ht.add("OVERLAPS");
        ht.add("OVERLAY");
        ht.add("OVERRIDING");
        ht.add("PACKED");
        ht.add("PAD");
        ht.add("PARAMETER");
        ht.add("PARAMETER_MODE");
        ht.add("PARAMETER_NAME");
        ht.add("PARAMETER_ORDINAL_POSITION");
        ht.add("PARAMETER_SPECIFIC_CATALOG");
        ht.add("PARAMETER_SPECIFIC_NAME");
        ht.add("PARAMETER_SPECIFIC_SCHEMA");
        ht.add("PARTIAL");
        ht.add("PARTITION");
        ht.add("PASCAL");
        ht.add("PATH");
        ht.add("PERCENTILE_CONT");
        ht.add("PERCENTILE_DISC");
        ht.add("PERCENT_RANK");
        ht.add("PIC");
        ht.add("PICTURE");
        ht.add("PLACING");
        ht.add("PLI");
        ht.add("POSITION");
        ht.add("POWER");
        ht.add("PRECEDING");
        ht.add("PRECISION");
        ht.add("PREPARE");
        ht.add("PRESERVE");
        ht.add("PRIMARY");
        ht.add("PRIOR");
        ht.add("PRIVILEGES");
        ht.add("PROCEDURE");
        ht.add("PUBLIC");
        ht.add("RANGE");
        ht.add("RANK");
        ht.add("READ");
        ht.add("READS");
        ht.add("REAL");
        ht.add("RECURSIVE");
        ht.add("REF");
        ht.add("REFERENCES");
        ht.add("REFERENCING");
        ht.add("REGR_AVGX");
        ht.add("REGR_AVGY");
        ht.add("REGR_COUNT");
        ht.add("REGR_INTERCEPT");
        ht.add("REGR_R2");
        ht.add("REGR_SLOPE");
        ht.add("REGR_SXX");
        ht.add("REGR_SXY");
        ht.add("REGR_SYY");
        ht.add("RELATIVE");
        ht.add("RELEASE");
        ht.add("REPEATABLE");
        ht.add("RESTART");
        ht.add("RESTRICT");
        ht.add("RESULT");
        ht.add("RETURN");
        ht.add("RETURNED_CARDINALITY");
        ht.add("RETURNED_LENGTH");
        ht.add("RETURNED_OCTET_LENGTH");
        ht.add("RETURNED_SQLSTATE");
        ht.add("RETURNS");
        ht.add("REVOKE");
        ht.add("RIGHT");
        ht.add("ROLE");
        ht.add("ROLLBACK");
        ht.add("ROLLUP");
        ht.add("ROUTINE");
        ht.add("ROUTINE_CATALOG");
        ht.add("ROUTINE_NAME");
        ht.add("ROUTINE_SCHEMA");
        ht.add("ROW");
        ht.add("ROWS");
        ht.add("ROW_COUNT");
        ht.add("ROW_NUMBER");
        ht.add("Rules");
        ht.add("SAVEPOINT");
        ht.add("SCALE");
        ht.add("SCHEMA");
        ht.add("SCHEMA_NAME");
        ht.add("SCOPE");
        ht.add("SCOPE_CATALOG");
        ht.add("SCOPE_NAME");
        ht.add("SCOPE_SCHEMA");
        ht.add("SCROLL");
        ht.add("SEARCH");
        ht.add("SECOND");
        ht.add("SECTION");
        ht.add("SECURITY");
        ht.add("See");
        ht.add("SELECT");
        ht.add("SELF");
        ht.add("SENSITIVE");
        ht.add("SEPARATE");
        ht.add("SEQUENCE");
        ht.add("SERIALIZABLE");
        ht.add("SERVER_NAME");
        ht.add("SESSION");
        ht.add("SESSION_USER");
        ht.add("SET");
        ht.add("SETS");
        ht.add("short");
        ht.add("SIGN");
        ht.add("SIMILAR");
        ht.add("SIMPLE");
        ht.add("SIZE");
        ht.add("SMALLINT");
        ht.add("SOME");
        ht.add("SOURCE");
        ht.add("SPACE");
        ht.add("SPECIFIC");
        ht.add("SPECIFICTYPE");
        ht.add("SPECIFIC_NAME");
        ht.add("SQL");
        ht.add("SQLEXCEPTION");
        ht.add("SQLSTATE");
        ht.add("SQLSTATE_TYPE");
        ht.add("SQLWARNING");
        ht.add("SQRT");
        ht.add("START");
        ht.add("STATE");
        ht.add("STATEMENT");
        ht.add("STATIC");
        ht.add("static");
        ht.add("STDDEV_POP");
        ht.add("STDDEV_SAMP");
        ht.add("STRUCTURE");
        ht.add("STYLE");
        ht.add("SUBCLASS_ORIGIN");
        ht.add("SUBMULTISET");
        ht.add("SUBSTRING");
        ht.add("SUM");
        ht.add("SYMMETRIC");
        ht.add("Syntax");
        ht.add("SYSTEM");
        ht.add("SYSTEM_USER");
        ht.add("TABLE");
        ht.add("TABLESAMPLE");
        ht.add("TABLE_NAME");
        ht.add("TEMPORARY");
        ht.add("the");
        ht.add("THEN");
        ht.add("TIES");
        ht.add("TIME");
        ht.add("TIMESTAMP");
        ht.add("TIMEZONE_HOUR");
        ht.add("TIMEZONE_MINUTE");
        ht.add("TO");
        ht.add("TOP_LEVEL_COUNT");
        ht.add("TRAILING");
        ht.add("TRANSACTION");
        ht.add("TRANSACTIONS_COMMITTED");
        ht.add("TRANSACTIONS_ROLLED_BACK");
        ht.add("TRANSACTION_ACTIVE");
        ht.add("TRANSFORM");
        ht.add("TRANSFORMS");
        ht.add("TRANSLATE");
        ht.add("TRANSLATION");
        ht.add("TREAT");
        ht.add("TRIGGER");
        ht.add("TRIGGER_CATALOG");
        ht.add("TRIGGER_NAME");
        ht.add("TRIGGER_SCHEMA");
        ht.add("TRIM");
        ht.add("TRUE");
        ht.add("TYPE");
        ht.add("UESCAPE");
        ht.add("UNBOUNDED");
        ht.add("UNCOMMITTED");
        ht.add("UNDER");
        ht.add("UNION");
        ht.add("UNIQUE");
        ht.add("UNKNOWN");
        ht.add("UNNAMED");
        ht.add("UNNEST");
        ht.add("unsigned");
        ht.add("UPDATE");
        ht.add("UPPER");
        ht.add("USAGE");
        ht.add("USER");
        ht.add("USER_DEFINED_TYPE_CATALOG");
        ht.add("USER_DEFINED_TYPE_CODE");
        ht.add("USER_DEFINED_TYPE_NAME");
        ht.add("USER_DEFINED_TYPE_SCHEMA");
        ht.add("USING");
        ht.add("VALUE");
        ht.add("VALUES");
        ht.add("VARCHAR");
        ht.add("VARYING");
        ht.add("VAR_POP");
        ht.add("VAR_SAMP");
        ht.add("VIEW");
        ht.add("volatile");
        ht.add("WHEN");
        ht.add("WHENEVER");
        ht.add("WHERE");
        ht.add("WIDTH_BUCKET");
        ht.add("WINDOW");
        ht.add("WITH");
        ht.add("WITHIN");
        ht.add("WITHOUT");
        ht.add("WORK");
        ht.add("WRITE");
        ht.add("YEAR");
        ht.add("ZONE");

        ResultSet rs = null;
        try {
            rs = stmt.executeQuery("GET RESERVED KW");
            while (rs.next()) {
                String keyword = rs.getString(1).toUpperCase();
                if (!ht.contains(keyword)) // only add to list if this is not a SQL 2003 kw
                    kws.add(keyword);
            }

            for (ii = 0, len = kws.size(); ii != len; ++ii) {
                sb.append(kws.get(ii));
                if (ii != len - 1)
                    sb.append(',');
            }

            sqlkws = sb.toString();
        } catch (Exception e) {
            /* fall back if db doesn't support the feature */
            sqlkws = "";
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException sqle) {}
            }
        }
        return sqlkws;
    }

    public String getNumericFunctions() throws SQLException {
        return "";
    }

    public String getStringFunctions() throws SQLException {
        return "";
    }

    public String getSystemFunctions() throws SQLException {
        return "";
    }

    public String getTimeDateFunctions() throws SQLException {
        return "";
    }

    public String getSearchStringEscape() throws SQLException {
        return null;
    }

    public String getExtraNameCharacters() throws SQLException {
        return "";
    }

    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        return true;
    }

    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        return true;
    }

    public boolean supportsColumnAliasing() throws SQLException {
        return true;
    }

    public boolean nullPlusNonNullIsNull() throws SQLException {
        return true;
    }

    public boolean supportsConvert() throws SQLException {
        return false;
    }

    public boolean supportsConvert(int fromType, int toType) throws SQLException {
        return false;
    }

    public boolean supportsTableCorrelationNames() throws SQLException {
        return false;
    }

    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        return false;
    }

    public boolean supportsExpressionsInOrderBy() throws SQLException {
        return true;
    }

    public boolean supportsOrderByUnrelated() throws SQLException {
        return false;
    }

    public boolean supportsGroupBy() throws SQLException {
        return true;
    }

    public boolean supportsGroupByUnrelated() throws SQLException {
        return true;
    }

    public boolean supportsGroupByBeyondSelect() throws SQLException {
        return true;
    }

    public boolean supportsLikeEscapeClause() throws SQLException {
        return true;
    }

    public boolean supportsMultipleResultSets() throws SQLException {
        return false;
    }

    public boolean supportsMultipleTransactions() throws SQLException {
        return true;
    }

    public boolean supportsNonNullableColumns() throws SQLException {
        return true;
    }

    public boolean supportsMinimumSQLGrammar() throws SQLException {
        return true;
    }

    public boolean supportsCoreSQLGrammar() throws SQLException {
        return true;
    }

    public boolean supportsExtendedSQLGrammar() throws SQLException {
        return false;
    }

    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        return false;
    }

    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        return false;
    }

    public boolean supportsANSI92FullSQL() throws SQLException {
        return false;
    }

    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        return false;
    }

    public boolean supportsOuterJoins() throws SQLException {
        return true;
    }

    public boolean supportsFullOuterJoins() throws SQLException {
        return false;
    }

    public boolean supportsLimitedOuterJoins() throws SQLException {
        return true;
    }

    public String getSchemaTerm() throws SQLException {
        return "schema";
    }

    public String getProcedureTerm() throws SQLException {
        return "procedure";
    }

    public String getCatalogTerm() throws SQLException {
        return "catalog";
    }

    public boolean isCatalogAtStart() throws SQLException {
        return true;
    }

    public String getCatalogSeparator() throws SQLException {
        return ".";
    }

    public boolean supportsSchemasInDataManipulation() throws SQLException {
        return false;
    }

    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        return false;
    }

    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        return false;
    }

    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        return false;
    }

    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        return false;
    }

    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        return false;
    }

    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        return false;
    }

    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        return false;
    }

    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    public boolean supportsPositionedDelete() throws SQLException {
        return false;
    }

    public boolean supportsPositionedUpdate() throws SQLException {
        return false;
    }

    public boolean supportsSelectForUpdate() throws SQLException {
        return true;
    }

    public boolean supportsStoredProcedures() throws SQLException {
        return true;
    }

    public boolean supportsSubqueriesInComparisons() throws SQLException {
        return true;
    }

    public boolean supportsSubqueriesInExists() throws SQLException {
        return true;
    }

    public boolean supportsSubqueriesInIns() throws SQLException {
        return true;
    }

    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        return false;
    }

    public boolean supportsCorrelatedSubqueries() throws SQLException {
        return false;
    }

    public boolean supportsUnion() throws SQLException {
        return true;
    }

    public boolean supportsUnionAll() throws SQLException {
        return true;
    }

    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        return false;
    }

    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        return false;
    }

    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        return false;
    }

    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        return false;
    }

    public int getMaxBinaryLiteralLength() throws SQLException {
        return 0;
    }

    public int getMaxCharLiteralLength() throws SQLException {
        return 0;
    }

    public int getMaxColumnNameLength() throws SQLException {
        return 0;
    }

    public int getMaxColumnsInGroupBy() throws SQLException {
        return 0;
    }

    public int getMaxColumnsInIndex() throws SQLException {
        return 0;
    }

    public int getMaxColumnsInOrderBy() throws SQLException {
        return 0;
    }

    public int getMaxColumnsInSelect() throws SQLException {
        return 0;
    }

    public int getMaxColumnsInTable() throws SQLException {
        return 0;
    }

    public int getMaxConnections() throws SQLException {
        return 0;
    }

    public int getMaxCursorNameLength() throws SQLException {
        return 0;
    }

    public int getMaxIndexLength() throws SQLException {
        return 0;
    }

    public int getMaxSchemaNameLength() throws SQLException {
        return 0;
    }

    public int getMaxProcedureNameLength() throws SQLException {
        return 0;
    }

    public int getMaxCatalogNameLength() throws SQLException {
        return 0;
    }

    public int getMaxRowSize() throws SQLException {
        return 0;
    }

    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        return false;
    }

    public int getMaxStatementLength() throws SQLException {
        return 0;
    }

    public int getMaxStatements() throws SQLException {
        return 0;
    }

    public int getMaxTableNameLength() throws SQLException {
        return 0;
    }

    public int getMaxTablesInSelect() throws SQLException {
        return 0;
    }

    public int getMaxUserNameLength() throws SQLException {
        return 0;
    }

    public int getDefaultTransactionIsolation() throws SQLException {
        /* TODO not really, but this is the closest answer for now */
        return Connection.TRANSACTION_READ_COMMITTED;
    }

    public boolean supportsTransactions() throws SQLException {
        return true;
    }

    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        return true;
    }

    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        /* TODO set to true after ddl feature is done */
        return false;
    }

    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        return false;
    }

    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        return false;
    }

    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        return false;
    }

    public ResultSet getProcedures(String catalog, String schemaPattern,
            String procedureNamePattern) throws SQLException {
        StringBuilder q = new StringBuilder();
        q.append("select ? as PROCEDURE_CAT,")
            .append("    ? as PROCEDURE_SCHEM,")
            .append("    name as PROCEDURE_NAME,")
            .append("    null as UNDEF1,")
            .append("    null as UNDEF2,")
            .append("    null as UNDEF3,")
            .append("    '' as REMARKS,")
            .append("    ? as PROCEDURE_TYPE,")
            .append("    (name || '.' || version) as SPECIFIC_NAME ")
            .append("from comdb2sys_procedures ");
        if (procedureNamePattern != null)
            q.append(" WHERE name LIKE ? ");
        q.append("order by PROCEDURE_CAT, PROCEDURE_SCHEM, PROCEDURE_NAME, SPECIFIC_NAME");

        if (ps != null)
            ps.close();

        ps = conn.prepareStatement(q.toString());
        ps.setString(1, conn.getDatabase());
        ps.setObject(2, null);
        ps.setInt(3, DatabaseMetaData.procedureResultUnknown);
        if (procedureNamePattern != null)
            ps.setString(4, procedureNamePattern);
        return ps.executeQuery();
    }

    public ResultSet getProcedureColumns(String catalog, String schemaPattern,
            String procedureNamePattern, String columnNamePattern) throws SQLException {
        /* TODO need a system table */
        return stmt.executeQuery("select null as PROCEDURE_CAT, " +
                "null as PROCEDURE_SCHEM, null as PROCEDURE_NAME, null as COLUMN_NAME, " +
                "null as COLUMN_TYPE, null as DATA_TYPE, null as TYPE_NAME, null as PRECISION, " +
                "null as LENGTH, null as SCALE, null as RADIX, null as NULLABLE, " +
                "null as REMARKS limit 0;");
    }

    public ResultSet getTables(String catalog, String schemaPattern,
            String tableNamePattern, String[] types) throws SQLException {
        tableNamePattern = (tableNamePattern == null || "".equals(tableNamePattern)) ? "%" : tableNamePattern;

        StringBuilder sql = new StringBuilder();
        sql.append("select null as TABLE_CAT,")
            .append("      null as TABLE_SCHEM,")
            .append("      name as TABLE_NAME,")
            .append("      upper(type) as TABLE_TYPE,")
            .append("      null as REMARKS,")
            .append("      null as TYPE_CAT,")
            .append("      null as TYPE_SCHEM,")
            .append("      null as TYPE_NAME,")
            .append("      null as SELF_REFERENCING_COL_NAME,")
            .append("      null as REF_GENERATION ")
            .append("      from (")
            .append("            select name,")
            .append("                   type")
            .append("            from sqlite_master")
            .append("           )")
            .append("where TABLE_NAME like ? ")
            .append("and   TABLE_TYPE in (");

        if (types == null || types.length == 0) {
            sql.append("'TABLE','VIEW'");
        } else {
            sql.append("'").append(types[0].toUpperCase()).append("'");

            for (int i = 1; i < types.length; i++) {
                sql.append(",'").append(types[i].toUpperCase()).append("'");
            }
        }

        sql.append(") order by TABLE_TYPE, TABLE_NAME");

        if (ps != null)
            ps.close();
        ps = conn.prepareStatement(sql.toString());
        ps.setString(1, tableNamePattern);

        return ps.executeQuery();
    }

    public ResultSet getSchemas() throws SQLException {
        if (ps != null)
            ps.close();
        ps = conn.prepareStatement("select null as TABLE_SCHEM, null as TABLE_CATALOG");
        return ps.executeQuery();
    }

    public ResultSet getCatalogs() throws SQLException {
        if (ps != null)
            ps.close();
        ps = conn.prepareStatement("select null as TABLE_CAT");
        return ps.executeQuery();
    }

    public ResultSet getTableTypes() throws SQLException {
        return stmt.executeQuery("select 'TABLE' as TABLE_TYPE union select 'VIEW' as TABLE_TYPE");
    }

    public ResultSet getColumns(String catalog, String schemaPattern,
            String tableNamePattern, String columnNamePattern) throws SQLException {
        StringBuilder q = new StringBuilder();
        q.append("select null as TABLE_CAT,")
            .append("    null as TABLE_SCHEM,")
            .append("    tablename as TABLE_NAME,")
            .append("    columnname as COLUMN_NAME,")
            .append("    0 as DATA_TYPE,")
            .append("    type as TYPE_NAME,")
            .append("    (size - 1) as COLUMN_SIZE,")
            .append("    0 as BUFFER_LENGTH,")
            .append("    0 as DECIMAL_DIGITS,")
            .append("    10 as NUM_PREC_RADIX,")
            .append("    (upper(isnullable) == 'Y') as NULLABLE,")
            .append("    null as REMARKS,")
            .append("    trim(defaultvalue) as COLUMN_DEF,")
            .append("    0 as SQL_DATA_TYPE,")
            .append("    0 as SQL_DATETIME_SUB,")
            .append("    (size - 1) as CHAR_OCTET_LENGTH,")
            .append("    0 as ORDINAL_POSITION,")
            .append("    CASE WHEN (upper(isnullable) == 'Y') THEN 'YES' ELSE 'NO' END as IS_NULLABLE,")
            .append("    null as SCOPE_CATALOG,")
            .append("    null as SCOPE_SCHEMA,")
            .append("    null as SCOPE_TABLE,")
            .append("    0 as SOURCE_DATA_TYPE,")
            .append("    'NO' as IS_AUTOINCREMENT,")
            .append("    'NO' as IS_GENERATEDCOLUMN,")
            .append("    sqltype ")
            .append("from comdb2sys_columns ")
            .append("where 1=1 AND ")
            .append("tablename LIKE ? AND ")
            .append("columnname LIKE ? ")
            .append("order by TABLE_CAT,TABLE_SCHEM, TABLE_NAME");

        if (ps != null)
            ps.close();
        ps = conn.prepareStatement(q.toString());
        ps.setString(1, tableNamePattern != null ? tableNamePattern : "%");
        ps.setString(2, columnNamePattern != null ? columnNamePattern : "%");

        return new Comdb2DatabaseMetaDataResultSet(ps.executeQuery());
    }

    public ResultSet getColumnPrivileges(String catalog, String schema,
            String table, String columnNamePattern) throws SQLException {
        return stmt.executeQuery("select null as TABLE_CAT, null as TABLE_SCHEM, " +
                "null as TABLE_NAME, null as COLUMN_NAME, null as GRANTOR, null as GRANTEE, " +
                "null as PRIVILEGE, null as IS_GRANTABLE limit 0;");
    }

    public ResultSet getTablePrivileges(String catalog, String schemaPattern,
            String tableNamePattern) throws SQLException {
        return stmt.executeQuery("select  null as TABLE_CAT, "
                + "null as TABLE_SCHEM, null as TABLE_NAME, null as GRANTOR, null "
                + "GRANTEE,  null as PRIVILEGE, null as IS_GRANTABLE limit 0;");
    }

    public ResultSet getBestRowIdentifier(String catalog, String schema,
            String table, int scope, boolean nullable) throws SQLException {
        return stmt.executeQuery("select " + bestRowSession + " as SCOPE, 'rowid' as COLUMN_NAME, " +
                java.sql.Types.BIGINT + " as DATA_TYPE, 'GENID' as TYPE_NAME, null as COLUMN_SIZE, " +
                "null as BUFFER_LENGTH, null as DECIMAL_DIGITS, " + bestRowPseudo + " as PSEUDO_COLUMN");
    }

    public ResultSet getVersionColumns(String catalog, String schema,
            String table) throws SQLException {
        return stmt.executeQuery("select null as SCOPE, null as COLUMN_NAME, "
                + "null as DATA_TYPE, null as TYPE_NAME, null as COLUMN_SIZE, "
                + "null as BUFFER_LENGTH, null as DECIMAL_DIGITS, null as PSEUDO_COLUMN limit 0");
    }

    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {

        StringBuilder q = new StringBuilder();

        if (ps != null)
            ps.close();

       q.append("select null as TABLE_CAT,")
           .append("    null as TABLE_SCHEM,")
           .append("    a.tablename as TABLE_NAME,")
           .append("    a.columnname as COLUMN_NAME,")
           .append("    (columnnumber + 1) as KEY_SEQ,")
           .append("    a.keyname as PK_NAME ")
           .append("from comdb2sys_keycomponents a,")
           .append("     comdb2sys_keys b ")
           .append("where ")
           .append("a.tablename = b.tablename and ")
           .append("a.keyname = b.keyname and ")
           .append("(upper(isunique) = 'Y' or upper(isunique) = 'YES') and ")
           .append("a.tablename like ? ")
           .append("order by a.tablename, a.columnname");
       ps = conn.prepareStatement(q.toString());

        ps.setString(1, table);

        return ps.executeQuery();
    }

    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        StringBuilder q = new StringBuilder();

        if (ps != null)
            ps.close();

        q.append("select null as PKTABLE_CAT,")
            .append("    null as PKTABLE_SCHEM,")
            .append("    c.tablename as PKTABLE_NAME,")
            .append("    c.columnname as PKCOLUMN_NAME,")
            .append("    null as FKTABLE_CAT,")
            .append("    null as FKTABLE_SCHEM,")
            .append("    a.tablename as FKTABLE_NAME,")
            .append("    a.columnname as FKCOLUMN_NAME,")
            .append("    (a.columnnumber + 1) as KEY_SEQ,")
            .append("    CASE WHEN (upper(iscascadingdelete)='YES' or upper(iscascadingdelete)='Y') THEN ? ELSE ? END as DELETE_RULE,")
            .append("    CASE WHEN (upper(iscascadingupdate)='YES' or upper(iscascadingupdate)='Y') THEN ? ELSE ? END as UPDATE_RULE,")
            .append("    a.keyname as FK_NAME,")
            .append("    c.keyname as PK_NAME,")
            .append("    ? as DEFERRABILITY ")
            .append("from comdb2sys_keycomponents a, comdb2sys_constraints b, comdb2sys_keycomponents c ")
            .append("where a.tablename = b.tablename and ")
            .append("      a.keyname = b.keyname and ")
            .append("      b.foreigntablename = c.tablename and ")
            .append("      b.foreignkeyname = c.keyname and ")
            .append("      b.tablename like ? ")
            .append("order by PKTABLE_NAME, KEY_SEQ");
        ps = conn.prepareStatement(q.toString());
        ps.setInt(1, importedKeyCascade);
        ps.setInt(2, importedKeyNoAction);
        ps.setInt(3, importedKeyCascade);
        ps.setInt(4, importedKeyNoAction);
        ps.setInt(5, importedKeyInitiallyDeferred);
        ps.setString(6, table);

        return ps.executeQuery();
    }

    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        StringBuilder q = new StringBuilder();

        if (ps != null)
            ps.close();

        q.append("select null as PKTABLE_CAT,")
            .append("    null as PKTABLE_SCHEM,")
            .append("    c.tablename as PKTABLE_NAME,")
            .append("    c.columnname as PKCOLUMN_NAME,")
            .append("    null as FKTABLE_CAT,")
            .append("    null as FKTABLE_SCHEM,")
            .append("    a.tablename as FKTABLE_NAME,")
            .append("    a.columnname as FKCOLUMN_NAME,")
            .append("    (a.columnnumber + 1) as KEY_SEQ,")
            .append("    CASE WHEN (upper(iscascadingdelete)='YES' or upper(iscascadingdelete)='Y') THEN ? ELSE ? END as DELETE_RULE,")
            .append("    CASE WHEN (upper(iscascadingupdate)='YES' or upper(iscascadingupdate)='Y') THEN ? ELSE ? END as UPDATE_RULE,")
            .append("    a.keyname as FK_NAME,")
            .append("    c.keyname as PK_NAME,")
            .append("    ? as DEFERRABILITY ")
            .append("from comdb2sys_keycomponents a, comdb2sys_constraints b, comdb2sys_keycomponents c ")
            .append("where a.tablename = b.tablename and ")
            .append("      a.keyname = b.keyname and ")
            .append("      b.foreigntablename = c.tablename and ")
            .append("      b.foreignkeyname = c.keyname and ")
            .append("      b.foreigntablename like ? ")
            .append("order by PKTABLE_NAME, KEY_SEQ");
        ps = conn.prepareStatement(q.toString());
        ps.setInt(1, importedKeyCascade);
        ps.setInt(2, importedKeyNoAction);
        ps.setInt(3, importedKeyCascade);
        ps.setInt(4, importedKeyNoAction);
        ps.setInt(5, importedKeyInitiallyDeferred);
        ps.setString(6, table);

        return ps.executeQuery();
    }

    public ResultSet getCrossReference(String parentCatalog,
            String parentSchema, String parentTable, String foreignCatalog,
            String foreignSchema, String foreignTable) throws SQLException {
        StringBuilder q = new StringBuilder();

        if (ps != null)
            ps.close();

        q.append("select null as PKTABLE_CAT,")
            .append("    null as PKTABLE_SCHEM,")
            .append("    c.tablename as PKTABLE_NAME,")
            .append("    c.columnname as PKCOLUMN_NAME,")
            .append("    null as FKTABLE_CAT,")
            .append("    null as FKTABLE_SCHEM,")
            .append("    a.tablename as FKTABLE_NAME,")
            .append("    a.columnname as FKCOLUMN_NAME,")
            .append("    (a.columnnumber + 1) as KEY_SEQ,")
            .append("    CASE WHEN (upper(iscascadingdelete)='YES' or upper(iscascadingdelete)='Y') THEN ? ELSE ? END as DELETE_RULE,")
            .append("    CASE WHEN (upper(iscascadingupdate)='YES' or upper(iscascadingupdate)='Y') THEN ? ELSE ? END as UPDATE_RULE,")
            .append("    a.keyname as FK_NAME,")
            .append("    c.keyname as PK_NAME,")
            .append("    ? as DEFERRABILITY ")
            .append("from comdb2sys_keycomponents a, comdb2sys_constraints b, comdb2sys_keycomponents c ")
            .append("where a.tablename = b.tablename and ")
            .append("      a.keyname = b.keyname and ")
            .append("      b.foreigntablename = c.tablename and ")
            .append("      b.foreignkeyname = c.keyname and ")
            .append("      b.foreigntablename like ? and ")
            .append("      b.tablename like ? ")
            .append("order by PKTABLE_NAME, KEY_SEQ");
        ps = conn.prepareStatement(q.toString());
        ps.setInt(1, importedKeyCascade);
        ps.setInt(2, importedKeyNoAction);
        ps.setInt(3, importedKeyCascade);
        ps.setInt(4, importedKeyNoAction);
        ps.setInt(5, importedKeyInitiallyDeferred);
        ps.setString(6, parentTable);
        ps.setString(7, foreignTable);

        return ps.executeQuery();
    }

    public ResultSet getTypeInfo() throws SQLException {
        return stmt.executeQuery("select " + "tn as TYPE_NAME, " + "dt as DATA_TYPE, "
                + "0 as PRECISION, " + "null as LITERAL_PREFIX, " + "null as LITERAL_SUFFIX, "
                + "null as CREATE_PARAMS, "
                + typeNullable
                + " as NULLABLE, "
                + "1 as CASE_SENSITIVE, "
                + typeSearchable
                + " as SEARCHABLE, "
                + "0 as UNSIGNED_ATTRIBUTE, "
                + "0 as FIXED_PREC_SCALE, "
                + "0 as AUTO_INCREMENT, "
                + "null as LOCAL_TYPE_NAME, "
                + "0 as MINIMUM_SCALE, "
                + "0 as MAXIMUM_SCALE, "
                + "0 as SQL_DATA_TYPE, "
                + "0 as SQL_DATETIME_SUB, "
                + "10 as NUM_PREC_RADIX from ("
                + "    select 'BLOB' as tn, "
                + Types.BLOB
                + " as dt union"
                + "    select 'NULL' as tn, "
                + Types.NULL
                + " as dt union"
                + "    select 'REAL' as tn, "
                + Types.REAL
                + " as dt union"
                + "    select 'DATETIME' as tn, "
                + Types.TIMESTAMP
                + " as dt union"
                + "    select 'TEXT' as tn, "
                + Types.VARCHAR
                + " as dt union"
                + "    select 'INTEGER' as tn, "
                + Types.INTEGER + " as dt" + ") order by TYPE_NAME;");
    }

    public ResultSet getIndexInfo(String catalog, String schema, String table,
            boolean unique, boolean approximate) throws SQLException {

        StringBuilder q = new StringBuilder();
        if (ps != null)
            ps.close();
        if (unique) {
            q.append("select null as TABLE_CAT,")
                .append("    null as TABLE_SCHEM,")
                .append("    a.tablename as TABLE_NAME,")
                .append("    (upper(isunique) = 'NO' or upper(isunique) = 'N') as NON_UNIQUE,")
                .append("    null as INDEX_QUALIFIER,")
                .append("    a.keyname as INDEX_NAME,")
                .append("    ? as TYPE,")
                .append("    (columnnumber + 1) as ORDINAL_POSITION,")
                .append("    a.columnname as COLUMN_NAME,")
                .append("    CASE WHEN (upper(isdescending) = 'NO' or upper(isdescending) = 'N') THEN 'A' ELSE 'D' END as ASC_OR_DESC,")
                .append("    0 as CARDINALITY,")
                .append("    0 as PAGES,")
                .append("    null as FILTER_CONDITION ")
                .append("from comdb2sys_keycomponents a, comdb2sys_keys b ")
                .append("where a.tablename = b.tablename and ")
                .append("      a.keyname = b.keyname and ")
                .append("      a.tablename like ? and ")
                .append("      (upper(isunique)='YES' or upper(isunique)='Y') ")
                .append("order by NON_UNIQUE, TYPE, INDEX_NAME, ORDINAL_POSITION");
            ps = conn.prepareStatement(q.toString());
            ps.setInt(1, tableIndexHashed);
            ps.setString(2, table);
        } else {
            q.append("select null as TABLE_CAT,")
                .append("    null as TABLE_SCHEM,")
                .append("    a.tablename as TABLE_NAME,")
                .append("    (upper(isunique) = 'NO' or upper(isunique) = 'N') as NON_UNIQUE,")
                .append("    null as INDEX_QUALIFIER,")
                .append("    a.keyname as INDEX_NAME,")
                .append("    ? as TYPE,")
                .append("    (columnnumber + 1) as ORDINAL_POSITION,")
                .append("    a.columnname as COLUMN_NAME,")
                .append("    CASE WHEN (upper(isdescending) = 'NO' or upper(isdescending) = 'N') THEN 'A' ELSE 'D' END as ASC_OR_DESC,")
                .append("    0 as CARDINALITY,")
                .append("    0 as PAGES,")
                .append("    null as FILTER_CONDITION ")
                .append("from comdb2sys_keycomponents a, comdb2sys_keys b ")
                .append("where a.tablename = b.tablename and ")
                .append("      a.keyname = b.keyname and ")
                .append("      a.tablename like ? ")
                .append("order by NON_UNIQUE, TYPE, INDEX_NAME, ORDINAL_POSITION");
            ps = conn.prepareStatement(q.toString());
            ps.setInt(1, tableIndexHashed);
            ps.setString(2, table);
        }

        return ps.executeQuery();
    }

    public boolean supportsResultSetType(int type) throws SQLException {
        return false;
    }

    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        return false;
    }

    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    public boolean ownDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    public boolean ownInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    public boolean othersDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    public boolean othersInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    public boolean updatesAreDetected(int type) throws SQLException {
        return false;
    }

    public boolean deletesAreDetected(int type) throws SQLException {
        return false;
    }

    public boolean insertsAreDetected(int type) throws SQLException {
        return false;
    }

    public boolean supportsBatchUpdates() throws SQLException {
        return true;
    }

    public ResultSet getUDTs(String catalog, String schemaPattern,
            String typeNamePattern, int[] types) throws SQLException {
        return stmt.executeQuery("select  null as TYPE_CAT, null as TYPE_SCHEM, "
                + "null as TYPE_NAME,  null as CLASS_NAME,  null as DATA_TYPE, null as REMARKS, "
                + "null as BASE_TYPE " + "limit 0;");
    }

    public Connection getConnection() throws SQLException {
        return conn;
    }

    public boolean supportsSavepoints() throws SQLException {
        return false;
    }

    public boolean supportsNamedParameters() throws SQLException {
        return true;
    }

    public boolean supportsMultipleOpenResults() throws SQLException {
        return false;
    }

    public boolean supportsGetGeneratedKeys() throws SQLException {
        return false;
    }

    public ResultSet getSuperTypes(String catalog, String schemaPattern,
            String typeNamePattern) throws SQLException {
        return stmt.executeQuery("select null as TYPE_CAT, null as TYPE_SCHEM, " +
                "null as TYPE_NAME, null as SUPERTYPE_CAT, null as SUPERTYPE_SCHEM, " +
                "null as SUPERTYPE_NAME limit 0");
    }

    public ResultSet getSuperTables(String catalog, String schemaPattern,
            String tableNamePattern) throws SQLException {
        return stmt.executeQuery("select null as TABLE_CAT, null as TABLE_SCHEM, " +
                "null as TABLE_NAME, null as SUPERTABLE_NAME limit 0");
    }

    public ResultSet getAttributes(String catalog, String schemaPattern,
            String typeNamePattern, String attributeNamePattern) throws SQLException {
        return stmt.executeQuery("select null as TYPE_CAT, null as TYPE_SCHEM, " +
                "null as TYPE_NAME, null as ATTR_NAME, null as DATA_TYPE, " +
                "null as ATTR_TYPE_NAME, null as ATTR_SIZE, null as DECIMAL_DIGITS, " +
                "null as NUM_PREC_RADIX, null as NULLABLE, null as REMARKS, null as ATTR_DEF, " +
                "null as SQL_DATA_TYPE, null as SQL_DATETIME_SUB, null as CHAR_OCTET_LENGTH, " +
                "null as ORDINAL_POSITION, null as IS_NULLABLE, null as SCOPE_CATALOG, " +
                "null as SCOPE_SCHEMA, null as SCOPE_TABLE, null as SOURCE_DATA_TYPE limit 0;");
    }

    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        return false;
    }

    public int getResultSetHoldability() throws SQLException {
        return 0;
    }

    public int getDatabaseMajorVersion() throws SQLException {
        return 0;
    }

    public int getDatabaseMinorVersion() throws SQLException {
        return 0;
    }

    public int getJDBCMajorVersion() throws SQLException {
        return 0;
    }

    public int getJDBCMinorVersion() throws SQLException {
        return 0;
    }

    public int getSQLStateType() throws SQLException {
        return 0;
    }

    public boolean locatorsUpdateCopy() throws SQLException {
        return false;
    }

    public boolean supportsStatementPooling() throws SQLException {
        return false;
    }

    public RowIdLifetime getRowIdLifetime() throws SQLException {
        return RowIdLifetime.ROWID_UNSUPPORTED;
    }

    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        return getSchemas();
    }

    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        return false;
    }

    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        return false;
    }

    public ResultSet getClientInfoProperties() throws SQLException {
        return stmt.executeQuery("select null as NAME, 0 as MAX_LEN, null as DEFAULT_VALUE, null as DESCRIPTION limit 0");
    }

    public ResultSet getFunctions(String catalog, String schemaPattern,
            String functionNamePattern) throws SQLException {
        return stmt.executeQuery("select null as FUNCTION_CAT, null as FUNCTION_SCHEM, null as FUNCTION_NAME, null as REMARKS, " 
                + "0 as FUNCTION_TYPE, null as SPECIFIC_NAME limit 0");
    }

    public ResultSet getFunctionColumns(String catalog, String schemaPattern,
            String functionNamePattern, String columnNamePattern) throws SQLException {
        return stmt.executeQuery("select null as FUNCTION_CAT, null as FUNCTION_SCHEM, null as FUNCTION_NAME, null as COLUMN_NAME, "
                + "0 as COLUMN_TYPE, 0 as DATA_TYPE, null as TYPE_NAME, 0 as PRECISION, 0 as LENGTH, 0 as SCALE, "
                + "0 as RADIX, 0 as NULLABLE, null as REMARKS, 0 as CHAR_OCTET_LENGTH, 0 as ORDINAL_POSITION, "
                + "'' as IS_NULLABLE, null as SPECIFIC_NAME limit 0");
    }

    public ResultSet getPseudoColumns(String catalog,
            String schemaPattern,
            String tableNamePattern,
            String columnNamePattern) throws SQLException {
        return stmt.executeQuery("select null as TABLE_CAT, null as TABLE_SCHEM, null as TABLE_NAME, null as COLUMN_NAME, "
                + "0 as DATA_TYPE, 0 as COLUMN_SIZE, 0 as DECIMAL_DIGITS, 10 as NUM_PREC_RADIX, null as COLUMN_USAGE, "
                + "null as REMARKS, 0 as CHAR_OCTET_LENGTH, '' as IS_NULLABLE limit 0");
    }

    public boolean generatedKeyAlwaysReturned() throws SQLException {
        return false;
    }
}
/* vim: set sw=4 ts=4 et: */
