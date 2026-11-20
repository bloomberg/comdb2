package com.bloomberg.comdb2.jdbc;

import java.sql.*;
import java.util.logging.*;
import org.junit.*;
import static org.junit.Assert.*;

public class DatabaseMetaDataTest {
    String db, cluster;
    Connection conn;
    Statement stmt;
    DatabaseMetaData dmd;
    ResultSet rs;
    
    @Before public void setUp() throws ClassNotFoundException, SQLException {
        db = System.getProperty("cdb2jdbc.test.database");
        cluster = System.getProperty("cdb2jdbc.test.cluster");
        conn = DriverManager.getConnection(String.format("jdbc:comdb2://%s/%s", cluster, db));
        stmt = conn.createStatement();
        stmt.executeUpdate("drop table if exists t2");
        stmt.executeUpdate("drop table if exists t1");
        conn.createStatement().execute("DROP TABLE if exists application");
        dmd = conn.getMetaData();
    }

    @After public void tearDown() throws SQLException {
        stmt.close();
        conn.close();
    }

    @Test public void testGetProcedures() throws SQLException {
        stmt.executeUpdate("create procedure sp1 {local function main() end}");
        stmt.executeUpdate("create procedure sp2 {local function main() end}");

        rs = dmd.getProcedures(null, null, "sp1");

        assertTrue(rs.next());
        String cat = rs.getString(1);
        String sche = rs.getString(2);
        String nm = rs.getString(3);

        assertEquals(cat, db);
        assertEquals(sche, null);
        assertEquals(nm, "sp1");
        rs.close();

        rs = dmd.getProcedures(null, null, "%");
        assertTrue(rs.next());
        assertTrue(rs.next());
        rs.close();

        rs = dmd.getProcedures(null, null, "sp3");
        assertFalse(rs.next());
        rs.close();
    }

    @Test public void testGetTables() throws SQLException {
        stmt.executeUpdate("create table t1 (i int, j text)");
        stmt.executeUpdate("create table t2 (i int, j text)");

        rs = dmd.getTables(null, null, "t1", null);

        assertTrue(rs.next());
        String cat = rs.getString(1);
        String sche = rs.getString(2);
        String nm = rs.getString(3);

        assertEquals(cat, null);
        assertEquals(sche, null);
        assertEquals(nm, "t1");
        assertFalse(rs.next());
        rs.close();

        rs = dmd.getTables(null, null, "t%", null);
        assertTrue(rs.next());
        assertTrue(rs.next());
        assertFalse(rs.next());
        rs.close();

        rs = dmd.getTables(null, null, "t3", null);
        assertFalse(rs.next());
        rs.close();

        stmt.executeUpdate("drop table t2");
        stmt.executeUpdate("drop table t1");
    }

    @Test public void testGetTablesResultSetSurvivesProductVersionLookup() throws SQLException {
        stmt.executeUpdate("create table t1 (i int, j text)");

        rs = dmd.getTables(null, null, "t1", null);
        String version = dmd.getDatabaseProductVersion();

        assertNotNull(version);
        assertTrue(rs.next());
        assertEquals(rs.getString("TABLE_NAME"), "t1");
        assertFalse(rs.next());
        rs.close();

        stmt.executeUpdate("drop table t1");
    }

    @Test public void testGetColumns() throws SQLException {
        stmt.executeUpdate("create table t1 (i int, j text)");
        stmt.executeUpdate("create table t2 (i int, j text)");

        rs = dmd.getColumns(null, null, "t1", "i");
        assertTrue(rs.next());
        String tbl = rs.getString("TABLE_NAME");
        String col = rs.getString("COLUMN_NAME");
        /* The 5th column is the SQL type. */
        int sqltype = rs.getInt(5);
        /* The 7th column is size. */
        int len = rs.getInt(7);
        assertEquals(tbl, "t1");
        assertEquals(col, "i");
        assertEquals(sqltype, java.sql.Types.INTEGER);
        assertEquals(len, 4); /* 4-byte integer */
        assertFalse(rs.next());
        rs.close();

        rs = dmd.getColumns(null, null, "t1", "%");
        assertTrue(rs.next());
        assertTrue(rs.next());
        assertFalse(rs.next());
        rs.close();

        rs = dmd.getColumns(null, null, "%", "j");
        assertTrue(rs.next());
        assertTrue(rs.next());
        assertFalse(rs.next());
        rs.close();

        stmt.executeUpdate("drop table t2");
        stmt.executeUpdate("drop table t1");
    }

    @Test public void testGetIndexes() throws SQLException {
        stmt.executeUpdate("create table t1 (i int, j int)");
        stmt.executeUpdate("create unique index t1i on t1(i)");
        stmt.executeUpdate("create index t1j on t1(j)");

        rs = dmd.getIndexInfo(null, null, "%", true, false);
        assertTrue(rs.next());
        String idx = rs.getString("INDEX_NAME");
        assertEquals(idx, "T1I");
        assertFalse(rs.next());
        rs.close();

        rs = dmd.getIndexInfo(null, null, "%", false, false);
        assertTrue(rs.next());
        idx = rs.getString("INDEX_NAME");
        assertEquals(idx, "T1I");
        assertTrue(rs.next());
        idx = rs.getString("INDEX_NAME");
        assertEquals(idx, "T1J");
        assertFalse(rs.next());
        rs.close();

        stmt.executeUpdate("drop table t1");
    }

    @Test public void testGetReferences() throws SQLException {
        stmt.executeUpdate("create table t1 (i int primary key)");
        stmt.executeUpdate("create table t2 (j int primary key, foreign key(j) references t1(i))");

        rs = dmd.getPrimaryKeys(null, null, "t1");
        assertTrue(rs.next());
        String tbl = rs.getString("TABLE_NAME");
        String col = rs.getString("COLUMN_NAME");
        assertEquals(tbl, "t1");
        assertEquals(col, "i");
        assertFalse(rs.next());
        rs.close();

        rs = dmd.getExportedKeys(null, null, "t1");
        assertTrue(rs.next());
        tbl = rs.getString("FKTABLE_NAME");
        col = rs.getString("FKCOLUMN_NAME");
        assertEquals(tbl, "t2");
        assertEquals(col, "j");
        assertFalse(rs.next());
        rs.close();

        rs = dmd.getExportedKeys(null, null, "t2");
        assertFalse(rs.next());
        rs.close();

        rs = dmd.getImportedKeys(null, null, "t2");
        assertTrue(rs.next());
        tbl = rs.getString("PKTABLE_NAME");
        col = rs.getString("PKCOLUMN_NAME");
        assertEquals(tbl, "t1");
        assertEquals(col, "i");
        assertFalse(rs.next());
        rs.close();

        rs = dmd.getImportedKeys(null, null, "t1");
        assertFalse(rs.next());
        rs.close();

        rs = dmd.getCrossReference(null, null, "%", null, null, "%");
        assertTrue(rs.next());
        tbl = rs.getString("PKTABLE_NAME");
        col = rs.getString("PKCOLUMN_NAME");
        assertEquals(tbl, "t1");
        assertEquals(col, "i");
        assertFalse(rs.next());
        rs.close();

        stmt.executeUpdate("drop table t2");
        stmt.executeUpdate("drop table t1");
    }

    @Test public void testGetSchemas() throws SQLException {
        rs = dmd.getSchemas();
        assertTrue(rs.next());
        String sche = rs.getString(1);
        String cat = rs.getString(2);
        assertEquals(cat, null);
        assertEquals(sche, null);
        assertFalse(rs.next());
        rs.close();
    }

    @Test public void testGetCatalogs() throws SQLException {
        rs = dmd.getCatalogs();
        assertTrue(rs.next());
        String cat = rs.getString(1);
        assertEquals(cat, null);
        assertFalse(rs.next());
        rs.close();
    }
}
