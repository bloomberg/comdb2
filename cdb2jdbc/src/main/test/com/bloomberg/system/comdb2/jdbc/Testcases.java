package com.bloomberg.system.comdb2.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import javax.sql.rowset.serial.SerialBlob;

import com.bloomberg.system.comdb2.jdbc.Cdb2Types.CString;
import com.bloomberg.system.comdb2.jdbc.Cdb2Types.Datetime;
import com.bloomberg.system.comdb2.jdbc.Cdb2Types.Int64;
import com.bloomberg.system.comdb2.jdbc.Cdb2Types.IntervalDaySecond;
import com.bloomberg.system.comdb2.jdbc.Cdb2Types.IntervalYearMonth;
import com.bloomberg.system.comdb2.jdbc.Cdb2Types.Real;
import com.bloomberg.system.comdb2.jdbc.Constants.Types;

public class Testcases {

	public static void jdbc(String db, String cluster)
			throws ClassNotFoundException, SQLException {
		/**
		 * Initialize comdb2 jdbc driver.
		 */
		Class.forName("com.bloomberg.system.comdb2.jdbc.Driver");

		/**
		 * Connect to a comdb2 instance. Format of connection url:
		 * jdbc:comdb2:<your_db_name>:<cluster_or_host> A valid cluster value
		 * could be "local", "dev", "alpha", "beta", or "prod". All other values
		 * shall be treated as a host name. The example below shows how to
		 * connect to a comdb2 instance running on host `ibm7'.
		 */
		Connection conn = DriverManager.getConnection(String.format(
				"jdbc:comdb2:%s:%s", db, cluster));

		/**
		 * Prepare a statement. To be consistent with C API, only named
		 * parameters are supported. In other words, question markers won't be
		 * replaced.
		 */
		PreparedStatement stmt = conn
				.prepareStatement("insert into comdb2developers values (@id, @name, @joined, @tax, @served, @time, @binary)");

		/**
		 * Because comdb2 responds long for any integral types, and double for
		 * float/double. So always use setLong() and setDouble() when binding
		 * parameters.
		 */

		/**
		 * Bind an integer value.
		 */
		stmt.setLong(1, (long) (Math.random() * 10000));
		/**
		 * Bind a string.
		 */
		stmt.setString(2, "jdbc prepared statement");
		/**
		 * Bind a datetime.
		 */
		stmt.setDate(3, new Date(System.currentTimeMillis()));

		/**
		 * The line below shows how to bind a null value. Do not use
		 * java.sql.Types for the 2nd parameter, use
		 * com.bloomberg.system.comdb2.jdbc.Constants.Types instead.
		 */
		// stmt.setNull(3, Constants.Types.CDB2_INTEGER);

		/**
		 * Bind a double value.
		 */
		stmt.setDouble(4, 99.99);

		/**
		 * JDBC does not have corresponding builtin types for intervalym and
		 * intervalds. setObject serves as a workaround.
		 */

		/**
		 * Bind a intervalym value (+3 mon).
		 */
		stmt.setObject(5, new Cdb2Types.IntervalYearMonth(1, 0, 3));
		/**
		 * Bind a intervalds value (+100 days).
		 */
		stmt.setObject(6, new Cdb2Types.IntervalDaySecond(1, 100, 0, 0, 0, 0));

		/**
		 * Bind a large object.
		 */
		stmt.setBlob(7, new SerialBlob("blahblahblah".getBytes()));

		/**
		 * Okay now execute it.
		 */
		int affected = stmt.executeUpdate();

		System.out.format("Successfully inserted %d row(s)\n", affected);

		/**
		 * A prepared statement can also execute external sql statements.
		 * However, the external query won't change the internal query. In other
		 * words, next time we call executeUpdate(), still the query `insert
		 * into...' will be executed.
		 */
		stmt.clearParameters();
		ResultSet rs = stmt.executeQuery("select * from comdb2developers");

		/**
		 * Select back.
		 */
		ResultSetMetaData md = rs.getMetaData();

		int cnt = md.getColumnCount();
		String[] colNames = new String[cnt];

		for (int i = 0; i != cnt; ++i)
			colNames[i] = md.getColumnName(i + 1);

		try {
			while (rs.next()) {
				for (int i = 0; i != cnt; ++i)
					System.out.format("%s=%s\t ", colNames[i],
							rs.getString(i + 1));
				System.out.println();
			}
		} catch (RuntimeException e) {
			System.out.println(e.getMessage());
		}

		rs.close();
		stmt.close();
		conn.close();
	}

	public static void native_api(String db, String cluster) throws NoDbHostFoundException, IOException {
		// 2014-12-31
		Datetime datime = new Datetime();
		datime.tm_year = 114;
		datime.tm_mon = 11;
		datime.tm_mday = 31;

		// - 10 years and 7 months
		IntervalYearMonth ym = new IntervalYearMonth();
		ym.sign = -1;
		ym.years = 12;
		ym.months = 10;

		// 100 days
		IntervalDaySecond ds = new IntervalDaySecond();
		ds.sign = -1;
		ds.days = 100;

		DbHandle hndl = new Comdb2Handle(db, cluster);
		hndl.runStatementWithException("begin");
		hndl.runStatementWithException("set transaction read committed");

		long random_integer = (long) (Math.random() * 10000);

		hndl.bindParameter("id", Types.CDB2_INTEGER,
				new Int64(random_integer).toBytes());
		hndl.bindParameter("name", Types.CDB2_CSTRING, new CString(
				"native api " + random_integer).toBytes());
		hndl.bindParameter("joined", Types.CDB2_DATETIME, datime.toBytes());
		hndl.bindParameter("tax", Types.CDB2_REAL, new Real(
				Math.random() * 10000).toBytes());
		hndl.bindParameter("served", Types.CDB2_INTERVALYM, ym.toBytes());
		hndl.bindParameter("time", Types.CDB2_INTERVALDS, ds.toBytes());
		hndl.runStatementWithException("insert into comdb2developers values(@id, @name, @joined, @tax, @served, @time, x'61626364')");
		hndl.clearParameters();

		hndl.runStatementWithException("select * from comdb2developers");

		int cols = hndl.numColumns();
		while (hndl.nextWithException()) {
			for (int i = 0; i != cols; ++i) {
				System.out.print(hndl.columnName(i) + "=");
				ByteArray ba = null;
				byte[] bytes = hndl.columnValue(i);

				switch (hndl.columnType(i)) {
				case Types.CDB2_INTEGER:
					ba = new Int64(bytes);
					break;
				case Types.CDB2_REAL:
					ba = new Real(bytes);
					break;
				case Types.CDB2_CSTRING:
					ba = new CString(bytes);
					break;
				case Types.CDB2_BLOB:
					ba = new Cdb2Types.Blob(bytes);
					break;
				case Types.CDB2_DATETIME:
					ba = new Datetime(bytes);
					break;
				case Types.CDB2_INTERVALYM:
					ba = new IntervalYearMonth(bytes);
					break;
				case Types.CDB2_INTERVALDS:
					ba = new IntervalDaySecond(bytes);
					break;
				}
				System.out.print(ba == null ? "<invalid type>" : ba.toString());
				System.out.print(", ");
			}
			System.out.print("\n");
		}
		hndl.runStatementWithException("commit");
		hndl.close();
	}

	public static void main(String[] args) throws ClassNotFoundException,
			SQLException, NoDbHostFoundException, IOException {
		
		if(args.length != 2) {
			System.err.println("No dbname and cluster provided.");
			return;
		}
		
		System.out.println("Executing jdbc example.");
		jdbc(args[0], args[1]);
		System.out.println("Executing native api example.");
		native_api(args[0], args[1]);
	}
}