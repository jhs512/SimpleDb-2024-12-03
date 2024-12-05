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

	public List<Map<String, Object>> runSelectRows(String sql, List<Object> params) {
		try {
			List<Map<String, Object>> mapList = excuteSelect(sql, params);

			return mapList;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public Map<String, Object> runSelectRow(String sql, List<Object> params) {
		try {
			List<Map<String, Object>> mapList = excuteSelect(sql, params);

			return mapList.get(0);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}

	}

	private List<Map<String, Object>> excuteSelect(String sql, List<Object> params) throws SQLException {
		Connection connection = createConnection();
		PreparedStatement preparedStatement = connection.prepareStatement(sql);

		setParams(params, preparedStatement);

		ResultSet resultSet = preparedStatement.executeQuery();
		ResultSetMetaData metaData = resultSet.getMetaData();

		List<Map<String, Object>> mapList = new LinkedList<>();

		while (resultSet.next()) {
				Map<String, Object> map = new HashMap<>();

				for (int i = 0; i < metaData.getColumnCount(); i++) {
					map.put(metaData.getColumnLabel(i + 1), resultSet.getObject(i + 1));
				}

				mapList.add(map);
			}
		preparedStatement.close();
		connection.close();
		return mapList;
	}

	public LocalDateTime runSelectDatetime(String sql, List<Object> params) {
		Map<String, Object> result = runSelectRow(sql, params);

		return result.containsKey("NOW()") ?
			LocalDateTime.parse(result.get("NOW()").toString()) : null;
	}

	public Long runSelectLong(String sql, List<Object> params) {
		Map<String, Object> result = runSelectRow(sql, params);

		String[] split = sql.split(" ");

		if (!result.containsKey(split[1])) {
			throw new RuntimeException("해당 데이터가 존재하지 않습니다.");
		}

		return Long.parseLong(result.get(split[1]).toString());
	}

	public String runSelectString(String sql, List<Object> params) {
		Map<String, Object> result = runSelectRow(sql, params);

		String[] split = sql.split(" ");

		if (!result.containsKey(split[1])) {
			throw new RuntimeException("해당 데이터가 존재하지 않습니다.");
		}

		return (String)result.get(split[1]);
	}

	public Boolean runSelectBoolean(String sql, List<Object> params) {
		Map<String, Object> result = runSelectRow(sql, params);
		StringBuilder sb = new StringBuilder();

		String[] split = sql.split(" ");

		for (int i = 1; i < split.length; i++) {
			if (split[i].equals("FROM")) {
				break;
			}
			sb.append(split[i] + " ");
		}

		sb.deleteCharAt(sb.length() - 1);

		if (!result.containsKey(sb.toString())) {
			throw new RuntimeException("해당 테이터가 존재하지 않습니다.");
		}

		Object obj = result.get(sb.toString());

		Boolean booleanValue;

		switch (obj) {
			case Boolean b -> booleanValue = (Boolean)obj;
			case String s -> booleanValue = "true".equals(obj.toString().toLowerCase());
			case Number n -> {
				Number number = (Number)result.get(sb.toString());

				booleanValue = number.intValue() == 1;
			}
			default -> booleanValue = Boolean.FALSE;
		}

		return booleanValue;
	}

	public List<Long> runSelectLongs(String sql, List<Object> params) {
		List<Map<String, Object>> mapList = runSelectRows(sql, params);

		StringBuilder sb = new StringBuilder();

		String[] split = sql.split(" ");

		for (int i = 1; i < split.length; i++) {
			if (split[i].equals("FROM")) {
				break;
			}
			sb.append(split[i] + " ");
		}

		sb.deleteCharAt(sb.length() - 1);
		String key = sb.toString();

		List<Long> result = new LinkedList<>();

		for (Map<String, Object> stringObjectMap : mapList) {
			if (stringObjectMap.containsKey(key)) {
				result.add((Long)stringObjectMap.get(key));
			}
		}

		if (result.isEmpty()) {
			throw new RuntimeException("해당 테이터가 없습니다.");
		}

		return result;
	}
}
