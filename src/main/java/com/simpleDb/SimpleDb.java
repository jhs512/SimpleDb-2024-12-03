package com.simpleDb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import javax.sql.DataSource;

import com.sql.Sql;

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

			connection.close();
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

	public Sql genSql() {

		return new Sql(this);
	}

	public long runInsert(String sql, List<Object> params) {
		long id = excuteUpdate(sql, params);

		return id;
	}

	public long excuteUpdate(String sql, List<Object> params) {
		try {
			Connection connection = createConnection();
			PreparedStatement preparedStatement = connection.prepareStatement(sql,
				PreparedStatement.RETURN_GENERATED_KEYS);


			if (!params.isEmpty()) {
				for (int i = 0; i < params.size(); i++) {
					preparedStatement.setObject(i + 1, params.get(i));
				}
			}

			long result = 0;

			ResultSet resultSet = preparedStatement.getGeneratedKeys();

			if (resultSet.next()) {
				result = resultSet.getLong(1);
			}

			preparedStatement.close();
			connection.close();

			return result;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
}
