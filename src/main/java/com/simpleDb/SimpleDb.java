package com.simpleDb;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

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
		excute(sql, List.of(parameters), PreparedStatement.NO_GENERATED_KEYS);
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
		long id = excute(sql, params, PreparedStatement.RETURN_GENERATED_KEYS);

		return id;
	}

	private long excute(String sql, List<Object> params, int generatedKey) {
		try {
			Connection connection = createConnection();

			PreparedStatement preparedStatement = connection.prepareStatement(sql, generatedKey);

			setParams(params, preparedStatement);

			long result = 0;

			if (generatedKey == PreparedStatement.RETURN_GENERATED_KEYS) {
				preparedStatement.executeUpdate();
				ResultSet resultSet = preparedStatement.getGeneratedKeys();

				if (resultSet.next()) {
					result = resultSet.getLong(1);
				}
			} else {
				result = preparedStatement.executeUpdate();
			}
			printSql(preparedStatement);

			preparedStatement.close();
			connection.close();

			return result;

		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private void setParams(List<Object> params, PreparedStatement preparedStatement) throws SQLException {
		for (int i = 0; i < params.size(); i++) {
			preparedStatement.setObject(i + 1, params.get(i));
		}
	}

	public long runUpdate(String sql, List<Object> params) {
		return excute(sql, params, PreparedStatement.NO_GENERATED_KEYS);
	}

	public List<Map<String, Object>> runSelectRows(String sql) {
		try {
			List<Map<String, Object>> mapList = new LinkedList<>();

			ResultSet resultSet = excuteSelect(sql);

			ResultSetMetaData metaData = resultSet.getMetaData();

			while (resultSet.next()) {
				Map<String, Object> map = new HashMap<>();

				for (int i = 0; i < metaData.getColumnCount(); i++) {
					map.put(metaData.getColumnLabel(i + 1), resultSet.getObject(i + 1));
				}

				mapList.add(map);
			}

			return mapList;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public Map<String, Object> runSelectRow(String sql) {
		try {
			ResultSet resultSet = excuteSelect(sql);

			ResultSetMetaData metaData = resultSet.getMetaData();

			Map<String, Object> map = new HashMap<>();

			while (resultSet.next()) {
				for (int i = 0; i < metaData.getColumnCount(); i++) {
					map.put(metaData.getColumnLabel(i + 1), resultSet.getObject(i + 1));
				}
			}

			return map;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}

	}

	private ResultSet excuteSelect(String sql) throws SQLException {
		Connection connection = createConnection();
		PreparedStatement preparedStatement = connection.prepareStatement(sql);
		ResultSet resultSet = preparedStatement.executeQuery(sql);
		return resultSet;
	}

	public LocalDateTime runSelectDatetime(String sql) {
		Map<String, Object> result = runSelectRow(sql);

		return result.containsKey("NOW()") ?
			LocalDateTime.parse(result.get("NOW()").toString()) : null;
	}

	public Long runSelectLong(String sql) {
		Map<String, Object> result = runSelectRow(sql);

		if (!result.containsKey("id")) {
			throw new RuntimeException("해당 아이디가 존재하지 않습니다.");
		}

		return Long.parseLong(result.get("id").toString());
	}

	public String runSelectString(String sql) {
		Map<String, Object> stringObjectMap = runSelectRow(sql);

		String[] split = sql.split(" ");

		return (String) stringObjectMap.get(split[1]);
	}
}
