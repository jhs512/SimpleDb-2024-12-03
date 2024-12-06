package com.ll;


import com.ll.util.Util;
import lombok.Setter;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;


public class SimpleDb {
    private final String host;
    private String user;
    private String password;
    private final String db;
    private String jdbcUrl;

    @Setter
    private boolean devMode;

    public SimpleDb(String host, String user, String password, String db) {
        this.host = host;
        this.user = user;
        this.password = password;
        this.db = db;
        this.jdbcUrl = "jdbc:mysql://" + host + ":3306/" + db;
    }

    private <T> T executeUpdate(PreparedStatement ps, Class<T> cls) throws SQLException {
        ps.executeUpdate();
        ResultSet generatedKeys = ps.getGeneratedKeys();
        if (generatedKeys.next() && cls == Long.class) {
            return cls.cast(generatedKeys.getLong(1));
        } else {
            return cls.cast(ps.getUpdateCount());
        }
    }

    private <T> T _run(String sql, Class<T> cls, Object... params) {
        Connection conn = getConnection();
        sql = sql.trim();

        try (PreparedStatement preparedStatement = conn.prepareStatement(sql, PreparedStatement.RETURN_GENERATED_KEYS)) {
            bindParams(preparedStatement, params);
            loggingSql(preparedStatement);

            if (sql.startsWith("INSERT") || sql.startsWith("UPDATE") || sql.startsWith("DELETE")) {
                return executeUpdate(preparedStatement, cls);
            } else if (sql.startsWith("SELECT")) {
                try (ResultSet rs = preparedStatement.executeQuery()) {
                    return parseResultSet(rs, cls);
                }
            }
            return cls.cast(preparedStatement.executeUpdate());
        } catch (SQLException e) {
            throw new RuntimeException("SQL 실행 실패 : " + e.getMessage(), e);
        }
    }

    private Map<String, Object> parseResulSetToMap(ResultSet rs) throws SQLException {
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();

        Map<String, Object> rows = new LinkedHashMap<>();
        for (int i = 1; i <= columnCount; i++) {
            String columnName = metaData.getColumnName(i);
            Object value = switch (metaData.getColumnType(i)) {
                case Types.BIGINT -> rs.getLong(columnName);
                case Types.BOOLEAN -> rs.getBoolean(columnName);
                case Types.TIMESTAMP -> {
                    Timestamp timestamp = rs.getTimestamp(columnName);
                    yield (timestamp != null) ? timestamp.toLocalDateTime() : null;
                }
                default -> rs.getObject(columnName);
            };
            rows.put(columnName, value);
        }
        return rows;
    }

    private <T> T parseResultSet(ResultSet rs, Class<T> cls) throws SQLException {
        if (!rs.next()) throw new NoSuchElementException("데이터 없음");
        Object value = switch (cls.getSimpleName()) {
            case "Long" -> rs.getLong(1);
            case "Boolean" -> rs.getBoolean(1);
            case "String" -> rs.getString(1);
            case "LocalDateTime" -> rs.getTimestamp(1).toLocalDateTime();
            case "Map" -> parseResulSetToMap(rs);
            case "List" -> {
                List<Map<String, Object>> rows = new ArrayList<>();
                do {
                    rows.add(parseResulSetToMap(rs));
                } while (rs.next());
                yield rows;
            }
            default -> throw new IllegalStateException("Unexpected value: " + cls.getSimpleName());
        };
        return cls.cast(value);
    }

    /*
     * sql 문 실행 후 반환 값이 int 라 long 사용 X
     * */
    public void run(String sql, Object... params) {
        _run(sql, Integer.class, params);
    }

    public Sql genSql() {
        return new Sql(this);
    }

    private final ThreadLocal<Connection> threadLocalConnection = ThreadLocal.withInitial(() -> {
        try {
            return DriverManager.getConnection(jdbcUrl, user, password);
        } catch (SQLException e) {
            throw new RuntimeException("Connection 생성 실패 : " + e.getMessage());
        }
    });

    public Connection getConnection() {
        return threadLocalConnection.get();
    }

    /*
     * devMode true 로 설정 시 생성된 sql문 출력
     *
     * @param ps Statement 객체
     * */
    private void loggingSql(Statement ps) {
        if (devMode) {
            System.out.println(ps);
        }
    }

    /*
     * 전달받은 PreparedStatement에 인자를 채움
     *
     * @param ps        PreparedStatement
     * @param params    Object[]
     * */
    private void bindParams(PreparedStatement ps, Object[] params) throws SQLException {
        // Column 은 1부터, 배열은 0부터 시작
        for (int i = 1; i <= params.length; i++) {
            ps.setObject(i, params[i - 1]);
        }
    }

    public void closeConnection() {
        Connection conn = threadLocalConnection.get();
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                throw new RuntimeException("커넥션 연결 실패 : " + e.getMessage());
            } finally {
                threadLocalConnection.remove();
            }
        }
    }

    public void startTransaction() {
        Connection conn = threadLocalConnection.get();
        try {
            if (conn.getAutoCommit()) {
                conn.setAutoCommit(false);
            }
        } catch (SQLException e) {
            throw new RuntimeException("트랜잭션 시작 실패 : " + e);
        }
    }

    public void rollback() {
        Connection conn = threadLocalConnection.get();
        if (conn != null) {
            try {
                conn.rollback();
                conn.setAutoCommit(true);
            } catch (SQLException e) {
                throw new RuntimeException("롤백 실패 : " + e);
            }
        }
    }

    public void commit() {
        Connection conn = threadLocalConnection.get();
        if (conn != null) {
            try {
                conn.commit();
                conn.setAutoCommit(true);
            } catch (SQLException e) {
                throw new RuntimeException("커밋 실패 : " + e);
            }
        }
    }

    public long insert(String sql, Object[] params) {
        return _run(sql, Long.class, params);
    }

    public int update(String sql, Object[] params) {
        return _run(sql, Integer.class, params);
    }

    public long delete(String sql, Object[] params) {
        return _run(sql, Integer.class, params);
    }

    public long selectLong(String sql, Object[] params) {
        return _run(sql, Long.class, params);
    }

    public Map<String, Object> selectRow(String sql, Object[] params) {
        return _run(sql, Map.class, params);
    }

    public <T> T selectRow(String sql, Class<T> tClass, Object[] params) {
        Map<String, Object> result = selectRow(sql, params);
        return Util.mapper.mapToObj(result, tClass);
    }

    public <T> List<T> selectRows(String sql, Class<T> tClass, Object[] params) {
        return selectRows(sql, params)
                .stream()
                .map(row -> (T) Util.mapper.mapToObj(row, tClass))
                .toList();
    }

    public List<Map<String, Object>> selectRows(String sql, Object[] params) {
        return _run(sql, List.class, params);
    }

    public Boolean selectBoolean(String sql, Object[] params) {
        return _run(sql, Boolean.class, params);
    }

    public String selectString(String sql, Object[] params) {
        return _run(sql, String.class, params);
    }

    public List<Long> selectLongs(String sql, Object[] params) {
        return selectRows(sql, params)
                .stream()
                .map(row -> (Long) row.values().iterator().next())
                .toList();
    }

    public LocalDateTime selectDateTime(String sql, Object[] params) {
        return _run(sql, LocalDateTime.class, params);
    }
}
