package com.bloomberg.system.comdb2.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class SqlTest {

	public static void main(String[] args) throws ClassNotFoundException,
			SQLException {
		Class.forName("com.bloomberg.system.comdb2.jdbc.Driver");
		Connection conn = DriverManager
				.getConnection("jdbc:comdb2:riversdb:ibm7");
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("select * from comdb2developers");
		ResultSetMetaData md = rs.getMetaData();

		int cnt = md.getColumnCount();
		String[] colNames = new String[cnt];

		for (int i = 0; i != cnt; ++i)
			colNames[i] = md.getColumnName(i + 1);

		while (rs.next()) {
			for (int i = 0; i != cnt; ++i)
				System.out.format("%s=%s\t ", colNames[i], rs.getString(i + 1));
			System.out.println();
		}
	}

}
