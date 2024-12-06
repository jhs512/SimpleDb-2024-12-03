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
    private final Map<String, Connection> connections = new HashMap<>();

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
            PreparedStatement preparedStatement = getCurrentThreadConnection().prepareStatement(query);
            for (int i = 0; i < params.length; i++) {
                preparedStatement.setObject(i + 1, params[i]);
            }

            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException();
        } finally {
            try {
                if (getCurrentThreadConnection().getAutoCommit()) {
                    clearCurrentThreadConnection();
                }
            } catch (SQLException e) {
                throw new RuntimeException();
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
            throw new RuntimeException();
        } finally {
            try {
                if (getCurrentThreadConnection().getAutoCommit()) {
                    clearCurrentThreadConnection();
                }
            } catch (SQLException e) {
                throw new RuntimeException();
            }
        }
        return -1;
    }

    public long runAndGetAffectedRowsCount(String query, Object... params) {
        try {
            return genPreparedStatement(query, params).executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException();
        } finally {
            try {
                if (getCurrentThreadConnection().getAutoCommit()) {
                    clearCurrentThreadConnection();
                }
            } catch (SQLException e) {
                throw new RuntimeException();
            }
        }
    }

    public List<Map<String, Object>> selectRows(String query, Object... params) {
        try {
            PreparedStatement preparedStatement = genPreparedStatement(query, params);
            ResultSet resultSet = preparedStatement.executeQuery();

            return genObjectMaps(resultSet);
        } catch (SQLException e) {
            throw new RuntimeException();
        } finally {
            try {
                if (getCurrentThreadConnection().getAutoCommit()) {
                    clearCurrentThreadConnection();
                }
            } catch (SQLException e) {
                throw new RuntimeException();
            }
        }
    }

    public Map<String, Object> selectRow(String query, Object... params) {
        try {
            PreparedStatement preparedStatement = genPreparedStatement(query, params);

            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                return genObjectMap(resultSet);
            }
        } catch (SQLException e) {
            throw new RuntimeException();
        } finally {
            try {
                if (getCurrentThreadConnection().getAutoCommit()) {
                    clearCurrentThreadConnection();
                }
            } catch (SQLException e) {
                throw new RuntimeException();
            }
        }
        return null;
    }

    public List<Object> selectRows(String query, Class<?> c, Object... params) {
        try {
            PreparedStatement preparedStatement = genPreparedStatement(query, params);
            ResultSet resultSet = preparedStatement.executeQuery();
            List<Object> result = new ArrayList<>();
            while (resultSet.next()) {
                result.add(genObject(c, resultSet));
            }

            return result;
        } catch (Exception e) {
            throw new RuntimeException();
        } finally {
            try {
                if (getCurrentThreadConnection().getAutoCommit()) {
                    clearCurrentThreadConnection();
                }
            } catch (SQLException e) {
                throw new RuntimeException();
            }
        }
    }

    public Object selectRow(String query, Class<?> c, Object... params) {
        try {
            PreparedStatement preparedStatement = genPreparedStatement(query, params);
            ResultSet resultSet = preparedStatement.executeQuery();

            if (resultSet.next()) {
                return genObject(c, resultSet);
            }
        } catch (Exception e) {
            throw new RuntimeException();
        } finally {
            try {
                if (getCurrentThreadConnection().getAutoCommit()) {
                    clearCurrentThreadConnection();
                }
            } catch (SQLException e) {
                throw new RuntimeException();
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
            throw new RuntimeException();
        }
    }

    public void rollback() {
        try {
            getCurrentThreadConnection().rollback();
        } catch (SQLException e) {
            throw new RuntimeException();
        }
    }

    public void commit() {
        try {
            getCurrentThreadConnection().commit();
            getCurrentThreadConnection().setAutoCommit(true);
        } catch (SQLException e) {
            throw new RuntimeException();
        }
    }

    private <T> T selectValue(String query, ResultSetExtractor<T> extractor, Object... params) {
        try {
            PreparedStatement preparedStatement = genPreparedStatement(query, params);
            ResultSet resultSet = preparedStatement.executeQuery();

            if (resultSet.next()) {
                return extractor.extract(resultSet);
            }
        } catch (SQLException e) {
            throw new RuntimeException();
        } finally {
            try {
                if (getCurrentThreadConnection().getAutoCommit()) {
                    clearCurrentThreadConnection();
                }
            } catch (SQLException e) {
                throw new RuntimeException();
            }
        }
        return null;
    }

    private <T> List<T> selectValues(String query, ResultSetExtractor<T> extractor, Object... params) {
        try {
            PreparedStatement preparedStatement = genPreparedStatement(query, params);
            List<T> values = new ArrayList<>();
            ResultSet resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {
                values.add(extractor.extract(resultSet));
            }

            return values;
        } catch (SQLException e) {
            throw new RuntimeException();
        } finally {
            try {
                if (getCurrentThreadConnection().getAutoCommit()) {
                    clearCurrentThreadConnection();
                }
            } catch (SQLException e) {
                throw new RuntimeException();
            }
        }
    }

    private PreparedStatement genPreparedStatement(String query, Object... params) throws SQLException {
        PreparedStatement preparedStatement = getCurrentThreadConnection().prepareStatement(query);

        for (int i = 0; i < params.length; i++) {
            preparedStatement.setObject(i + 1, params[i]);
        }

        return preparedStatement;
    }

    private PreparedStatement genPreparedStatement(String query, int statementType, Object... params) throws SQLException {
        PreparedStatement preparedStatement = getCurrentThreadConnection().prepareStatement(query, statementType);

        for (int i = 0; i < params.length; i++) {
            preparedStatement.setObject(i + 1, params[i]);
        }

        return preparedStatement;
    }

    private Object genObject(Class<?> c, ResultSet resultSet) throws Exception {
        Object object = c.getConstructor().newInstance();

        for (Field field : c.getDeclaredFields()) {
            field.setAccessible(true);
            String type = field.getType().getSimpleName();
            String fieldName = field.getName();

            switch (type) {
                case "Long" -> {
                    field.set(object, resultSet.getLong(fieldName));
                }
                case "String" -> {
                    field.set(object, resultSet.getString(fieldName));
                }
                case "LocalDateTime" -> {
                    field.set(object, resultSet.getTimestamp(fieldName).toLocalDateTime());
                }
                case "Boolean" -> {
                    field.set(object, resultSet.getBoolean(fieldName));
                }
            }
        }

        return object;
    }

    private List<Map<String, Object>> genObjectMaps(ResultSet resultSet) throws SQLException {
        List<Map<String, Object>> results = new ArrayList<>();

        while (resultSet.next()) {
            results.add(genObjectMap(resultSet));
        }

        return results;
    }

    private Map<String, Object> genObjectMap(ResultSet resultSet) throws SQLException {
        Map<String, Object> result = new HashMap<>();
        ResultSetMetaData metaData = resultSet.getMetaData();

        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            String columnName = metaData.getColumnName(i);
            Object value = switch(metaData.getColumnType(i)) {
                case Types.BIGINT -> resultSet.getLong(columnName);
                case Types.TIMESTAMP -> resultSet.getTimestamp(columnName).toLocalDateTime();
                case Types.BOOLEAN -> resultSet.getBoolean(columnName);
                default -> resultSet.getObject(columnName);
            };

            result.put(columnName, value);
        }
        return result;
    }

    @FunctionalInterface
    private interface ResultSetExtractor<T> {
        T extract(ResultSet resultSet) throws SQLException;
    }
}
