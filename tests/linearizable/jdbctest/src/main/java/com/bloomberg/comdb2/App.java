package com.bloomberg.comdb2;
import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;
import com.jcraft.jsch.*;

/**
 * Hello world!
 *
 */
public class App
{
    public static void main(String[] args)
        throws SQLException, ClassNotFoundException, InterruptedException,
               JSchException, IOException
    {

        InsertTest insertTest = new InsertTest();
        insertTest.runInsertTest();
    }
}
