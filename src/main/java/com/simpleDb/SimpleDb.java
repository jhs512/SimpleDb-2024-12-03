package com.simpleDb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.sql.Sql;

public class SimpleDb {
	private static final String URL_PREFIX = "jdbc:mysql://";
	private static final String DATABASE_PORT = ":3306/";
	private String url;
	private String username;
	private String password;
	private Boolean devMode;
	private final ObjectMapper objectMapper;
	private final ThreadLocal<Connection> threadLocal = new ThreadLocal<>();

	public SimpleDb(String url, String username, String password, String database) {
		this.url = URL_PREFIX + url + DATABASE_PORT + database;
		this.username = username;
		this.password = password;
		objectMapper = new ObjectMapper()
			.registerModule(new JavaTimeModule())
			.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
	}

	public void setDevMode(Boolean devMode) {
		this.devMode = devMode;
	}

	public void run(String sql, Object... parameters) {
		excute(sql, List.of(parameters), PreparedStatement.NO_GENERATED_KEYS);
	}

	private void printSql(PreparedStatement preparedStatement) {
		if (devMode) {
			String preparedStatementString = preparedStatement.toString();
			String sql = preparedStatementString
				.substring(preparedStatementString.indexOf(':') + 2, preparedStatementString.length());

			System.out.println("== rawSql ==");
			System.out.println(sql);
		}
	}

	private Connection getConnection() {
		try {
			if (threadLocal.get() == null || threadLocal.get().isClosed()) {
				threadLocal.set(createConnection());
			}
			return threadLocal.get();
		} catch (SQLException e) {
			throw new RuntimeException(e);
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

	/**
	 * 데이터 추가 메소드
	 * @param sql SQL
	 * @param params 파라미터 값
	 * @return {@link Long}
	 */
	public long runInsert(String sql, List<Object> params) {
		long id = excute(sql, params, PreparedStatement.RETURN_GENERATED_KEYS);

		return id;
	}

	/**
	 * 실제 DB에 결과 값을 가져오는 메소드
	 * @param sql SQL
	 * @param params 파라미터 값
	 * @param generatedKey 리턴 값
	 * @return {@link Long}
	 */
	private long excute(String sql, List<Object> params, int generatedKey) {
		try {
			Connection connection = getConnection();

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

			return result;

		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
	/**
	 * 데이터 수정 메소드
	 * @param sql SQL
	 * @param params 파라미터 값
	 * @return {@link Long}
	 */
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

	/**
	 * 한개 데이터 불러오는 메소드
	 * @param sql SQL
	 * @param clazz 변환할 Class
	 * @return {@link T} 파라미터로 들어온 Class로 반환
 	 */
	public <T> T runSelectRow(String sql, Class<T> clazz) {
		T result = null;
		List<T> t = runSelectRows(sql, clazz);
		if (t.isEmpty()) {
			throw new RuntimeException("해당 데이터가 존재하지 않습니다.");
		}
		result = t.get(0);
		return result;
	}

	/**
	 * 여러개의 데이터를 불러오는 메소드
	 * @param sql SQL
	 * @param clazz 변환할 Class
	 * @return {@link List<T>} 파라미터로 들어온 Class를 List에 담아서 반환
 	 */
	public <T> List<T> runSelectRows(String sql, Class<T> clazz) {
		List<T> mapList = new ArrayList<>();
		try {
			mapList = excuteSelect(sql, clazz);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		return mapList;
	}

	/**
	 * DB에서 데이터를 가져와서 List로 변한하는 메소드
	 * @param sql SQL
	 * @param clazz 변환할 Class
	 * @return {@link T} 파라미터로 들어온 Class로 반한
 	 */
	private <T> List<T> excuteSelect(String sql, Class<T> clazz) throws SQLException {
		ResultSet resultSet = getResultSet(sql);
		ResultSetMetaData metaData = resultSet.getMetaData();

		List<T> list = new LinkedList<>();
		while (resultSet.next()) {
			Map<String, Object> map = new HashMap<>();
			for (int i = 0; i < metaData.getColumnCount(); i++) {
				map.put(metaData.getColumnLabel(i + 1), resultSet.getObject(i + 1));
			}
			list.add(objectMapper.convertValue(map, clazz));
		}

		return list;
	}

	/**
	 * DB에서 값을 가져와서 데이터 가공후 반한하는 메소드
	 * @param sql SQL
	 * @param params 파라미터 값
	 * @return {@link List}
	 * @throws SQLException
	 */
	private List<Map<String, Object>> excuteSelect(String sql, List<Object> params) throws SQLException {
		ResultSet resultSet = getResultSet(sql, params);
		ResultSetMetaData metaData = resultSet.getMetaData();

		List<Map<String, Object>> mapList = new LinkedList<>();

		while (resultSet.next()) {
			Map<String, Object> map = new HashMap<>();

			for (int i = 0; i < metaData.getColumnCount(); i++) {
				map.put(metaData.getColumnLabel(i + 1), resultSet.getObject(i + 1));
			}

			mapList.add(map);
		}

		return mapList;
	}

	/**
	 * DB기준 현재 시간을 가져오는 메소드
	 * @param sql SQL
	 * @param params 파라미터 값
	 * @return {@link LocalDateTime}
 	 */
	public LocalDateTime runSelectDatetime(String sql, List<Object> params) {
		Map<String, Object> result = runSelectRow(sql, params);

		return result.containsKey("NOW()") ?
			LocalDateTime.parse(result.get("NOW()").toString()) : null;
	}

	/**
	 * 결과 값이 숫자인 데이터 가져오는 메소드
	 * @param sql
	 * @param params
	 * @return {@link Long}
	 */
	public Long runSelectLong(String sql, List<Object> params) {
		Map<String, Object> result = runSelectRow(sql, params);

		String[] split = sql.split(" ");

		if (!result.containsKey(split[1])) {
			throw new RuntimeException("해당 데이터가 존재하지 않습니다.");
		}

		return Long.parseLong(result.get(split[1]).toString());
	}

	/**
	 * 결과 값이 String인 데이터를 가져오는 메소드
	 * @param sql
	 * @param params
	 * @return {@link String}
	 */
	public String runSelectString(String sql, List<Object> params) {
		Map<String, Object> result = runSelectRow(sql, params);

		String[] split = sql.split(" ");

		if (!result.containsKey(split[1])) {
			throw new RuntimeException("해당 데이터가 존재하지 않습니다.");
		}

		return (String)result.get(split[1]);
	}

	/**
	 * 결과 값이 Boolean인 데이터를 가져오는 메소드
	 * @param sql
	 * @param params
	 * @return {@link Boolean}
	 */
	public Boolean runSelectBoolean(String sql, List<Object> params) {
		Map<String, Object> result = runSelectRow(sql, params);
		String key = getKey(sql);

		if (!result.containsKey(key)) {
			throw new RuntimeException("해당 테이터가 존재하지 않습니다.");
		}

		Object obj = result.get(key);

		Boolean booleanValue;

		switch (obj) {
			case Boolean b -> booleanValue = (Boolean)obj;
			case String s -> booleanValue = "true".equals(obj.toString().toLowerCase());
			case Number n -> {
				Number number = (Number)result.get(key);

				booleanValue = number.intValue() == 1;
			}
			default -> booleanValue = Boolean.FALSE;
		}

		return booleanValue;
	}

	/**
	 * 결과 값이 Long인 데이터 여러개를 가져오는 메소드
	 * @param sql
	 * @param params
	 * @return {@link List<Long>}
	 */
	public List<Long> runSelectLongs(String sql, List<Object> params) {
		List<Map<String, Object>> mapList = runSelectRows(sql, params);

		String key = getKey(sql);

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

	/**
	 * preparedStatement에 파라미터 바인딩하는 메소드
	 * @param params
	 * @param preparedStatement
	 * @throws SQLException
	 */
	private void setParams(List<Object> params, PreparedStatement preparedStatement) throws SQLException {
		for (int i = 0; i < params.size(); i++) {
			preparedStatement.setObject(i + 1, params.get(i));
		}
	}

	/**
	 * 쿼리를 DB에 보내고 결과 값을 반환해주는 메소드
	 * @param sql
	 * @return {@link ResultSet}
	 * @throws SQLException
	 */
	private ResultSet getResultSet(String sql) throws SQLException {
		Connection connection = getConnection();
		PreparedStatement preparedStatement = connection.prepareStatement(sql);
		printSql(preparedStatement);
		ResultSet resultSet = preparedStatement.executeQuery();
		return resultSet;
	}

	/**
	 * 쿼리를 DB에 보내고 결과 값을 반환해주는 메소드
	 * @param sql
	 * @param params
	 * @return
	 * @throws SQLException
	 */
	private ResultSet getResultSet(String sql, List<Object> params) throws SQLException {
		Connection connection = getConnection();
		PreparedStatement preparedStatement = connection.prepareStatement(sql);
		setParams(params, preparedStatement);
		printSql(preparedStatement);
		ResultSet resultSet = preparedStatement.executeQuery();
		return resultSet;
	}

	/**
	 * sql에 SELECT한 컬럼명 가져오는 메소드
	 * @param sql
	 * @return {@link String}
	 */
	private String getKey(String sql) {
		StringBuilder sb = new StringBuilder();

		String[] split = sql.split(" ");

		for (int i = 1; i < split.length; i++) {
			if (split[i].equals("FROM")) {
				break;
			}
			sb.append(split[i] + " ");
		}

		sb.deleteCharAt(sb.length() - 1);
		return sb.toString();
	}

	/**
	 * 사용한 Connection을 close하는 메소드
	 * @throws RuntimeException
	 */
	public void closeConnection() {
		Connection connection = threadLocal.get();
		try {
			if (connection != null || !connection.isClosed()) {
				connection.close();
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * 트랜잭션 시작 메소드
	 */
	public void startTransaction() {
		Connection connection = getConnection();
		try {
			connection.setAutoCommit(false);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * 롤백 메소드
	 */
	public void rollback() {
		Connection connection = getConnection();
		try {
			connection.rollback();
			connection.setAutoCommit(true);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * 커밋 메소드
	 */
	public void commit() {
		Connection connection = getConnection();
		try {
			connection.commit();
			connection.setAutoCommit(true);
		} catch (SQLException e) {
			rollback();
			throw new RuntimeException();
		}
	}
}
