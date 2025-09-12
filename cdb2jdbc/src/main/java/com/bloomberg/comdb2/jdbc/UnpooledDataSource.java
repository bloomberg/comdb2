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

import javax.sql.DataSource;
import org.slf4j.Logger;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.locks.ReentrantLock;

//@formatter:off
/**
 * Credits to mybatis.
 * http://grepcode.com/file/repo1.maven.org/maven2/org.mybatis/mybatis/3.0.4/org/apache/ibatis/datasource/unpooled/UnpooledDataSource.java
 * 
 * Credits to apache dbcp for initialization sql logic
 * https://github.com/apache/commons-dbcp/blob/master/src/main/java/org/apache/commons/dbcp2/BasicDataSource.java#L1201
 * 
 * @author Sebastien Blind
 *
 */
//@formatter:on

public class UnpooledDataSource implements DataSource {

	private ClassLoader driverClassLoader;
	private Properties driverProperties;
	private boolean driverInitialized;
	private final ReentrantLock driverLock = new ReentrantLock();

	private String driver;
	private String url;
	private String username;
	private String password;

	private boolean autoCommit = true; // Connections should default to auto commit
	private Integer defaultTransactionIsolationLevel;
	private List<String> connectionInitSqls;

	public UnpooledDataSource() {
	}

	public UnpooledDataSource(String driver, String url, String username, String password) {
		this.driver = driver;
		this.url = url;
		this.username = username;
		this.password = password;
	}

	public UnpooledDataSource(String driver, String url, Properties driverProperties) {
		this.driver = driver;
		this.url = url;
		this.driverProperties = driverProperties;
	}

	public UnpooledDataSource(ClassLoader driverClassLoader, String driver, String url, String username, String password) {
		this.driverClassLoader = driverClassLoader;
		this.driver = driver;
		this.url = url;
		this.username = username;
		this.password = password;
	}

	public UnpooledDataSource(ClassLoader driverClassLoader, String driver, String url, Properties driverProperties) {
		this.driverClassLoader = driverClassLoader;
		this.driver = driver;
		this.url = url;
		this.driverProperties = driverProperties;
	}

	public Connection getConnection() throws SQLException {
		initializeDriver();
		Connection connection;
		if (driverProperties != null) {
			connection = DriverManager.getConnection(url, driverProperties);
		} else if (username == null && password == null) {
			connection = DriverManager.getConnection(url);
		} else {
			connection = DriverManager.getConnection(url, username, password);
		}
		configureConnection(connection);
		return connection;
	}

	public Connection getConnection(String username, String password) throws SQLException {
		initializeDriver();
		Connection connection = DriverManager.getConnection(url, username, password);
		configureConnection(connection);
		return connection;
	}

	public void setLoginTimeout(int loginTimeout) throws SQLException {
		DriverManager.setLoginTimeout(loginTimeout);
	}

	public int getLoginTimeout() throws SQLException {
		return DriverManager.getLoginTimeout();
	}

	public void setLogWriter(PrintWriter logWriter) throws SQLException {
		DriverManager.setLogWriter(logWriter);
	}

	public PrintWriter getLogWriter() throws SQLException {
		return DriverManager.getLogWriter();
	}

	public ClassLoader getDriverClassLoader() {
		return driverClassLoader;
	}

	public void setDriverClassLoader(ClassLoader driverClassLoader) {
		this.driverClassLoader = driverClassLoader;
	}

	public Properties getDriverProperties() {
		return driverProperties;
	}

	public void setDriverProperties(Properties driverProperties) {
		this.driverProperties = driverProperties;
	}

	public String getDriver() {
		driverLock.lock();

		try {
			return driver;
		} finally {
			driverLock.unlock();
		}
	}

	public void setDriver(String driver) {
		driverLock.lock();

		try {
			this.driver = driver;
			driverInitialized = false;
		} finally {
			driverLock.unlock();
		}
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public boolean isAutoCommit() {
		return autoCommit;
	}

	public void setAutoCommit(boolean autoCommit) {
		this.autoCommit = autoCommit;
	}

	public Integer getDefaultTransactionIsolationLevel() {
		return defaultTransactionIsolationLevel;
	}

	public void setDefaultTransactionIsolationLevel(Integer defaultTransactionIsolationLevel) {
		this.defaultTransactionIsolationLevel = defaultTransactionIsolationLevel;
	}
	
	/**
     * Returns the list of SQL statements executed when a physical connection is first created. Returns an empty list if
     * there are no initialization statements configured.
     *
     * @return initialization SQL statements
     */
    public List<String> getConnectionInitSqls() {
        final List<String> result = connectionInitSqls;
        if (result == null) {
            return Collections.emptyList();
        }
        return result;
    }

    /**
     * Sets the list of SQL statements to be executed when a physical connection is first created (i.e. every time).
     *
     * @param connectionInitSqls
     *            Collection of SQL statements to execute on connection creation
     */
	public void setConnectionInitSqls(final Collection<String> connectionInitSqls) {
        if (connectionInitSqls != null && connectionInitSqls.size() > 0) {
            ArrayList<String> newVal = null;
            for (final String s : connectionInitSqls) {
                if (s != null && s.trim().length() > 0) {
                    if (newVal == null) {
                        newVal = new ArrayList<String>();
                    }
                    newVal.add(s);
                }
            }
            this.connectionInitSqls = newVal;
        } else {
            this.connectionInitSqls = null;
        }
    }

	private void configureConnection(Connection conn) throws SQLException {
		if (autoCommit != conn.getAutoCommit()) {
			conn.setAutoCommit(autoCommit);
		}
		if (defaultTransactionIsolationLevel != null) {
			conn.setTransactionIsolation(defaultTransactionIsolationLevel);
		}

		if (connectionInitSqls != null) {
			Statement stmt = null;
			try {
				stmt = conn.createStatement();
				for (String sql : connectionInitSqls) {
					stmt.execute(sql);
				}
			} finally {
				if (stmt != null) {
					try {
						stmt.close();
					} catch (Exception t) {
						// ignored
					}
				}
			}
		}
	}

	private void initializeDriver() {
		driverLock.lock();

		try {
			lockedInitializeDriver();
		} finally {
			driverLock.unlock();
		}
	}

	private void lockedInitializeDriver() {
		if (!driverInitialized) {
			driverInitialized = true;
			Class<?> driverType;
			try {
				if (driverClassLoader != null) {
					driverType = Class.forName(driver, true, driverClassLoader);
				} else {
					driverType = Class.forName(driver);
				}

				DriverManager.registerDriver(new DriverProxy((java.sql.Driver) driverType.newInstance()));
			} catch (Exception e) {
				throw new RuntimeException("Error setting driver (" + driver + ") on UnpooledDataSource. Cause: " + e, e);
			}
		}
	}

	private static class DriverProxy implements java.sql.Driver {
		private java.sql.Driver driver;

		DriverProxy(java.sql.Driver d) {
			this.driver = d;
		}

		public boolean acceptsURL(String u) throws SQLException {
			return this.driver.acceptsURL(u);
		}

		public Connection connect(String u, Properties p) throws SQLException {
			return this.driver.connect(u, p);
		}

		public int getMajorVersion() {
			return this.driver.getMajorVersion();
		}

		public int getMinorVersion() {
			return this.driver.getMinorVersion();
		}

		public DriverPropertyInfo[] getPropertyInfo(String u, Properties p) throws SQLException {
			return this.driver.getPropertyInfo(u, p);
		}

		public boolean jdbcCompliant() {
			return this.driver.jdbcCompliant();
		}

		public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
			throw new SQLFeatureNotSupportedException();
		}
	}

	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
		throw new SQLFeatureNotSupportedException();
	}
}
/* vim: set sw=4 ts=4 et: */
