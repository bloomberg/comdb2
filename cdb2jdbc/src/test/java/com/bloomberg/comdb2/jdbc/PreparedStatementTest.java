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

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/*
 * @author Junmin Liu
 */
public class PreparedStatementTest {
    final static String INSERT_APPLICATION= "insert into application(app_id, name, update_uuid, update_tms) values ((select coalesce(max(app_id), 0)+1 from application), @name, @uuid, @tms)";
    final static String SELECT_APPLICATION_BY_NAME_TMS="SELECT app_id as id, name, update_uuid AS uuid, update_tms as tms FROM APPLICATION WHERE name =@name and update_tms=@tms";
    final static String DELETE_APPLICATION_BY_ID="delete from APPLICATION WHERE app_id =@id";
    static String db;
    static String cluster;

    Connection conn;
    int appId;


    @Before
    public void setUp() throws ClassNotFoundException, SQLException{
        db = System.getProperty("cdb2jdbc.test.database");
        cluster = System.getProperty("cdb2jdbc.test.cluster");
        conn = DriverManager.getConnection(String.format("jdbc:comdb2://%s/%s", cluster, db));
    }

    @After
    public void tearDown() throws SQLException{
        if(appId == 0)
            return;
        /*
         * clean up the inserted row
         */
        PreparedStatement stmt = conn.prepareStatement(DELETE_APPLICATION_BY_ID);
        stmt.setInt(1, appId);
        int affected = stmt.executeUpdate();
        assertEquals("should delete one row", 1, affected );

        stmt.clearParameters();
    }

    @Test
    public void createTable() throws SQLException {
        try {
            conn.createStatement().execute("DROP TABLE application");
        } catch (SQLException sqle) {
            /* Ignore */
        }
        PreparedStatement stmt = conn.prepareStatement(
                "create table application (app_id int primary key, name varchar(100), update_uuid int, update_tms datetime)");
        int shouldbezero = stmt.executeUpdate();
        assertEquals("Expecting 0 from a DDL statement.", 0, shouldbezero);
    }

    /*
     * autoCommit default is true
     */
    @Test
    public void insertSelectDelete() throws ClassNotFoundException, SQLException{

        Date date = new Date(System.currentTimeMillis());
        String appName = "cdb2jdbctest" + System.currentTimeMillis();

        PreparedStatement stmt = conn.prepareStatement(INSERT_APPLICATION);

        stmt.setString(1, appName);
        stmt.setInt(2, 1234);	
        stmt.setDate(3, date);

        int affected = stmt.executeUpdate();

        assertEquals("should insert one row", 1, affected );

        /**
         * A prepared statement can also execute external sql statements.
         * However, the external query won't change the internal query. In other
         * words, next time we call executeUpdate(), still the query `insert
         * into...' will be executed.
         */
        stmt.clearParameters();

        stmt = conn.prepareStatement(SELECT_APPLICATION_BY_NAME_TMS);

        stmt.setString(1, appName);
        stmt.setDate(2, date);

        ResultSet rs = stmt.executeQuery();

        while(rs.next()){
            appId = rs.getInt(1);
            assertTrue("should get application id", appId>0);
            assertEquals("should get the appName back", appName, rs.getString(2));			
            Date d = rs.getDate(4);
            assertEquals("java.sql.Date", d.getClass().getName());
            assertTrue(d.compareTo(date)==0);

        }

        stmt.clearParameters();
    }

    /*
     * explicit set autocomit to false and call connection.commit 
     */
    @Test
    public void setAutoCommitFalseThenCommit() throws ClassNotFoundException, SQLException{
        Date date = new Date(System.currentTimeMillis());
        String appName = "cdb2jdbctest" + System.currentTimeMillis();

        conn.setAutoCommit(false);

        PreparedStatement stmt = conn.prepareStatement(INSERT_APPLICATION);

        stmt.setString(1, appName);
        stmt.setInt(2, 1234);	
        stmt.setDate(3, date);

        int affected = stmt.executeUpdate();

        assertEquals("should insert one row", 1, affected );

        conn.commit();

        stmt.clearParameters();
        /*
         * start checking
         */
        conn.setAutoCommit(true);

        stmt = conn.prepareStatement(SELECT_APPLICATION_BY_NAME_TMS);

        stmt.setString(1, appName);
        stmt.setDate(2, date);

        ResultSet rs = stmt.executeQuery();
        while(rs.next()){
            appId = rs.getInt(1);
            assertTrue("should get application id", appId>0);
            assertEquals("should get the appName back", appName, rs.getString(2));
        }

        stmt.clearParameters();

    }

    @Test
    public void setAutoCommitFalseThenRollback() throws SQLException, ClassNotFoundException {
        Date date = new Date(System.currentTimeMillis());
        String appName = "cdb2jdbctest" + System.currentTimeMillis();

        conn.setAutoCommit(false);

        PreparedStatement stmt = conn.prepareStatement(INSERT_APPLICATION);

        stmt.setString(1, appName);
        stmt.setInt(2, 1234);	
        stmt.setDate(3, date);

        try{
            int affected = stmt.executeUpdate();
            assertEquals("should insert one row", 1, affected );
            stmt.clearParameters();
            /*
             * intentionally set value for only one param to trigger SQLException
             * so we can test conn.rollback()
             */
            stmt.setString(1, appName);
            stmt.executeUpdate();	
            /*
             * test case will fail here
             */
            assertTrue(false);
        }catch(SQLException e){
            conn.rollback();
        }

        stmt.clearParameters();	
        /*
         * start checking
         */
        conn.setAutoCommit(true);
        stmt = conn.prepareStatement(SELECT_APPLICATION_BY_NAME_TMS);
        stmt.setString(1, appName);
        stmt.setDate(2, date);
        ResultSet rs = stmt.executeQuery();
        assertFalse(rs.next());		

        stmt.clearParameters();	
    }
    
    /*
     * test case insensitive column names
     */
    @Test
    public void selectCaseSensitiveColumn() throws ClassNotFoundException, SQLException{
        PreparedStatement stmt = conn.prepareStatement("select 1 id");
        
        ResultSet rs = stmt.executeQuery();
        rs.next();
        
        int id = rs.getInt("ID");
        assertEquals(id, 1);
        
        stmt.clearParameters();
    }
}
/* vim: set sw=4 ts=4 et: */
