package com.simpleDb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import javax.sql.DataSource;

public class SimpleDb {
	private static final String URL_PREFIX = "jdbc:mysql://";
	private static final String DATABASE_PORT = ":3306/";
	private String url;
	private String username;
	private String password;
	private Boolean devMode;

	public SimpleDb(String url, String username, String password, String database) {
		this.url = URL_PREFIX + url + DATABASE_PORT + database;
		this.username = username;
		this.password = password;
	}

	public void setDevMode(Boolean devMode) {
		this.devMode = devMode;
	}

	public void run(String sql, Object... parameters) {
		Connection connection = null;
		try {
			connection = createConnection();

			PreparedStatement preparedStatement = connection.prepareStatement(sql);
			if (!(parameters == null) || !(parameters.length == 0)) {
				for (int i = 0; i < parameters.length; i++) {
					preparedStatement.setObject(i + 1, parameters[i]);
				}
			}

			printSql(preparedStatement);

			preparedStatement.execute();
			preparedStatement.close();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private void printSql(PreparedStatement preparedStatement) {
		if (devMode) {
			System.out.println(preparedStatement.toString());
		}
	}

	public Connection createConnection() throws SQLException {
		try {
			Connection connection = DriverManager.getConnection(url, username, password);
			return connection;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
}
