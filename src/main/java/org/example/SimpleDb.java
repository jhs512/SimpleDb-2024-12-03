package org.example;

import java.lang.reflect.Field;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;

public class SimpleDb {
    String host;
    String user;
    String password;
    String name;
    private final int port = 3306;
    private boolean devMode;
    private Map<String, Connection> connections = new HashMap<>();
    private Map<String, Boolean> isTransaction = new HashMap<>();

    public SimpleDb(String host, String user, String password, String name) {
        this.host = host;
        this.user = user;
        this.password = password;
        this.name = name;
    }

    private Connection getCurrentThreadConnection() {
        String currentThread = Thread.currentThread().getName();
        Connection connection = connections.get(currentThread);

        if (connection != null) {
            return connection;
        }

        String url = "jdbc:mysql://" + host + ":" + port + "/" + name;
        try {
            connection = DriverManager.getConnection(url, user, password);
        } catch (SQLException e) {
            throw new RuntimeException();
        }

        connections.put(currentThread, connection);
        return connection;
    }

    private void clearCurrentThreadConnection() {
        String currentThread = Thread.currentThread().getName();
        Connection connection = connections.get(currentThread);

        if (connection == null) {
            return;
        }

        try {
            connection.close();
        } catch (SQLException e) {
            throw new RuntimeException();
        } finally {
            connections.remove(currentThread);
        }
    }

    public void closeConnection() {
        clearCurrentThreadConnection();
    }

    public Sql genSql() {
        return new Sql(this);
    }

    public void setDevMode(boolean isDevMode) {
        this.devMode = isDevMode;
    }

    public void run(String query, Object... params) {
        try {
            PreparedStatement pstmt = getCurrentThreadConnection().prepareStatement(query);
            for (int i = 0; i < params.length; i++) {
                pstmt.setObject(i + 1, params[i]);
            }

            pstmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (getCurrentThreadConnection().getAutoCommit()) {
                    clearCurrentThreadConnection();
                }
            } catch (SQLException e) {
                new RuntimeException();
            }
        }
    }

    public long runAndGetGeneratedKey(String query, Object... params) {
        try {
            PreparedStatement preparedStatement = genPreparedStatement(query, Statement.RETURN_GENERATED_KEYS, params);
            preparedStatement.executeUpdate();

            try (ResultSet generatedKeys = preparedStatement.getGeneratedKeys()) {
                if (generatedKeys.next()) {
                    return generatedKeys.getLong(1);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (getCurrentThreadConnection().getAutoCommit()) {
                    clearCurrentThreadConnection();
                }
            } catch (SQLException e) {
                new RuntimeException();
            }
        }
        return -1;
    }

    public long runAndGetAffectedRowsCount(String query, Object... params) {
        try {
            return genPreparedStatement(query, params).executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (getCurrentThreadConnection().getAutoCommit()) {
                    clearCurrentThreadConnection();
                }
            } catch (SQLException e) {
                new RuntimeException();
            }
        }
        return -1;
    }

    public List<Map<String, Object>> selectRows(String query, Object... params) {
        try {
            PreparedStatement preparedStatement = genPreparedStatement(query, params);

            ResultSet rs = preparedStatement.executeQuery();
            List<Map<String, Object>> results = new ArrayList<>();
            while (rs.next()) {
                Map<String, Object> result = new HashMap<>();
                result.put("id", rs.getLong("id"));
                result.put("title", rs.getString("title"));
                result.put("body", rs.getString("body"));
                result.put("createdDate", rs.getTimestamp("createdDate").toLocalDateTime());
                result.put("modifiedDate", rs.getTimestamp("modifiedDate").toLocalDateTime());
                result.put("isBlind", rs.getBoolean("isBlind"));

                results.add(result);
            }

            return results;
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (getCurrentThreadConnection().getAutoCommit()) {
                    clearCurrentThreadConnection();
                }
            } catch (SQLException e) {
                new RuntimeException();
            }
        }
        return null;
    }

    public Object selectRow(String query, Class c, Object... params) {
        try {
            PreparedStatement preparedStatement = genPreparedStatement(query, params);
            ResultSet rs = preparedStatement.executeQuery();
            if (rs.next()) {
                return genObject(c, rs);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (getCurrentThreadConnection().getAutoCommit()) {
                    clearCurrentThreadConnection();
                }
            } catch (SQLException e) {
                new RuntimeException();
            }
        }
        return null;
    }

    public List<Object> selectRows(String query, Class c, Object... params) {
        try {
            PreparedStatement preparedStatement = genPreparedStatement(query, params);
            ResultSet rs = preparedStatement.executeQuery();
            List<Object> result = new ArrayList<>();
            while (rs.next()) {
                result.add(genObject(c, rs));
            }

            return result;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (getCurrentThreadConnection().getAutoCommit()) {
                    clearCurrentThreadConnection();
                }
            } catch (SQLException e) {
                new RuntimeException();
            }
        }
        return null;
    }

    public Map<String, Object> selectRow(String query, Object... params) {
        try {
            PreparedStatement preparedStatement = genPreparedStatement(query, params);

            ResultSet rs = preparedStatement.executeQuery();
            Map<String, Object> result = new HashMap<>();
            if (rs.next()) {
                result.put("id", rs.getLong("id"));
                result.put("title", rs.getString("title"));
                result.put("body", rs.getString("body"));
                result.put("createdDate", rs.getTimestamp("createdDate").toLocalDateTime());
                result.put("modifiedDate", rs.getTimestamp("modifiedDate").toLocalDateTime());
                result.put("isBlind", rs.getBoolean("isBlind"));
            }

            return result;
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (getCurrentThreadConnection().getAutoCommit()) {
                    clearCurrentThreadConnection();
                }
            } catch (SQLException e) {
                new RuntimeException();
            }
        }
        return null;
    }

    public LocalDateTime selectDatetime(String query, Object... params) {
        return selectValue(query,
                resultSet -> resultSet.getTimestamp(1).toLocalDateTime(),
                params
        );
    }

    public Long selectLong(String query, Object... params) {
        return selectValue(query,
                resultSet -> resultSet.getLong(1),
                params
        );
    }

    public String selectString(String query, Object... params) {
        return selectValue(query,
                resultSet -> resultSet.getString(1),
                params
        );
    }

    public Boolean selectBoolean(String query, Object... params) {
        return selectValue(query,
                resultSet -> resultSet.getBoolean(1),
                params
        );
    }

    public List<Long> selectLongs(String query, Object... params) {
        return selectValues(query,
                resultSet -> resultSet.getLong(1),
                params);
    }

    public void startTransaction() {
        try {
            getCurrentThreadConnection().setAutoCommit(false);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void rollback() {
        try {
            getCurrentThreadConnection().rollback();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void commit() {
        try {
            getCurrentThreadConnection().commit();
            getCurrentThreadConnection().setAutoCommit(true);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private <T> T selectValue(String query, ResultSetExtractor<T> extractor, Object... params) {
        try {
            PreparedStatement preparedStatement = genPreparedStatement(query, params);

            ResultSet rs = preparedStatement.executeQuery();
            if (rs.next()) {
                return extractor.extract(rs);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (getCurrentThreadConnection().getAutoCommit()) {
                    clearCurrentThreadConnection();
                }
            } catch (SQLException e) {
                new RuntimeException();
            }
        }
        return null;
    }

    private <T> List<T> selectValues(String query, ResultSetExtractor<T> extractor, Object... params) {
        try {
            PreparedStatement preparedStatement = genPreparedStatement(query, params);
            List<T> values = new ArrayList<>();

            ResultSet rs = preparedStatement.executeQuery();
            while (rs.next()) {
                values.add(extractor.extract(rs));
            }

            return values;
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (getCurrentThreadConnection().getAutoCommit()) {
                    clearCurrentThreadConnection();
                }
            } catch (SQLException e) {
                new RuntimeException();
            }
        }
        return null;
    }

    private PreparedStatement genPreparedStatement(String query, Object... params) throws SQLException {
        PreparedStatement preparedStatement = getCurrentThreadConnection().prepareStatement(query);

        for (int i = 0; i < params.length; i++) {
            preparedStatement.setObject(i + 1, params[i]);
        }

        return preparedStatement;
    }

    private PreparedStatement genPreparedStatement(String query, int statement, Object... params) throws SQLException {
        PreparedStatement preparedStatement = getCurrentThreadConnection().prepareStatement(query, statement);

        for (int i = 0; i < params.length; i++) {
            preparedStatement.setObject(i + 1, params[i]);
        }

        return preparedStatement;
    }

    private Object genObject(Class c, ResultSet rs) throws Exception {
        Object object = c.getConstructor().newInstance();

        for (Field field : c.getDeclaredFields()) {
            field.setAccessible(true);
            String type = field.getType().getSimpleName();
            String fieldName = field.getName();

            switch (type) {
                case "Long" -> {
                    field.set(object, rs.getLong(fieldName));
                }
                case "String" -> {
                    field.set(object, rs.getString(fieldName));
                }
                case "LocalDateTime" -> {
                    field.set(object, rs.getTimestamp(fieldName).toLocalDateTime());
                }
                case "Boolean" -> {
                    field.set(object, rs.getBoolean(fieldName));
                }
            }
        }

        return object;
    }

    @FunctionalInterface
    private interface ResultSetExtractor<T> {
        T extract(ResultSet rs) throws SQLException;
    }
}
