package com.bloomberg.comdb2.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class StmtReturnTypesTest {

    private String db;
    private String cluster;
    private Connection conn;

    @Before
    public void setUp() throws SQLException {
        db = System.getProperty("cdb2jdbc.test.database");
        cluster = System.getProperty("cdb2jdbc.test.cluster");
        conn = DriverManager.getConnection(
                String.format("jdbc:comdb2://%s/%s", cluster, db));
    }

    @After
    public void tearDown() throws SQLException {
        if (conn != null)
            conn.close();
    }

    @Test
    public void allTypes() throws SQLException {
        String[] typeNames = {
            "INTEGER", "REAL", "CSTRING", "BLOB", "DATETIME",
            "INTERVALYM", "INTERVALDS", "DATETIMEUS", "INTERVALDSUS"
        };
        int[] expectedSql = {
            java.sql.Types.BIGINT,
            java.sql.Types.REAL,
            java.sql.Types.VARCHAR,
            java.sql.Types.BLOB,
            java.sql.Types.TIMESTAMP,
            java.sql.Types.OTHER,    // intervalym
            java.sql.Types.OTHER,    // intervalds
            java.sql.Types.TIMESTAMP,
            java.sql.Types.OTHER,    // intervaldsus
        };

        StringBuilder setSql = new StringBuilder("set statement return types");
        StringBuilder selectSql = new StringBuilder("select");
        String comma = "";
        for (String t : typeNames) {
            setSql.append(' ').append(t);
            selectSql.append(comma).append(" NULL");
            comma = ",";
        }

        Statement stmt = conn.createStatement();
        stmt.execute(setSql.toString());
        ResultSet rs = stmt.executeQuery(selectSql.toString());
        ResultSetMetaData md = rs.getMetaData();
        assertEquals("column count", typeNames.length, md.getColumnCount());
        for (int i = 0; i < typeNames.length; ++i) {
            assertEquals("col " + i + " (" + typeNames[i] + ")",
                    expectedSql[i], md.getColumnType(i + 1));
        }
        rs.close();
        stmt.close();
    }

    @Test
    public void doubleSet() throws SQLException {
        Statement stmt = conn.createStatement();
        stmt.execute("set statement return types cstring");
        try {
            stmt.execute("set statement return types cstring");
            fail("expected SQLException when setting return types twice");
        } catch (SQLException expected) {
            /* expected */
        }
    }

    @Test
    public void badType() throws SQLException {
        Statement stmt = conn.createStatement();
        try {
            stmt.execute("set statement return types deadbeef");
            fail("expected SQLException for unknown type");
        } catch (SQLException expected) {
            /* expected */
        }
        stmt.close();
    }

    @Test
    public void noTypes() throws SQLException {
        Statement stmt = conn.createStatement();
        try {
            stmt.execute("set statement return types");
            fail("expected SQLException when no types are given");
        } catch (SQLException expected) {
            /* expected */
        }
        stmt.close();
    }

    @Test
    public void oneShot() throws SQLException {
        Statement stmt = conn.createStatement();
        stmt.execute("set statement return types cstring");

        ResultSet rs = stmt.executeQuery("select 1");
        assertEquals(java.sql.Types.VARCHAR, rs.getMetaData().getColumnType(1));
        rs.close();

        rs = stmt.executeQuery("select 1");
        assertEquals("type override should not be carried across statements",
                java.sql.Types.BIGINT, rs.getMetaData().getColumnType(1));
        rs.close();
        stmt.close();
    }

    @Test
    public void inTransaction() throws SQLException {
        conn.setAutoCommit(false);
        try {
            Statement stmt = conn.createStatement();
            stmt.execute("set statement return types cstring");
            ResultSet rs = stmt.executeQuery("select 1");
            assertEquals(java.sql.Types.BIGINT, rs.getMetaData().getColumnType(1));
            rs.close();

            conn.commit();
            stmt.close();
            fail("expected SQLException when setting return types in a transaction");
        } catch (SQLException expected) {
            /* expected */
        } finally {
            conn.setAutoCommit(true);
        }
    }
}
