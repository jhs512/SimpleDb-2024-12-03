package com.ll.simpleDb;

import lombok.RequiredArgsConstructor;

import java.sql.*;
import java.util.*;

@RequiredArgsConstructor
public class SimpleDb {
    private final String host;
    private final String id;
    private final String pw;
    private final String db;

    private final Map<String, Connection> connections = new HashMap<>();

    private String createDatabaseUrl() {
        return String.format("jdbc:mysql://%s:3306/%s", host, db);
    }

    private Connection getCurrentThreadConnection() {
        Connection connection = connections.get(Thread.currentThread().getName());

        if (connection != null) return connection;
        try {
            connection = DriverManager.getConnection(createDatabaseUrl(), id, pw);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to connect to database", e);
        }
        connections.put(Thread.currentThread().getName(), connection);

        return connection;
    }

//    private void connect() {
//        if (connection == null) {
//            try {
//                connection = DriverManager.getConnection(createDatabaseUrl(), id, pw);
//            } catch (SQLException e) {
//                throw new RuntimeException("Failed to connect to database", e);
//            }
//        }
//    }

    public Sql genSql() {

        return new Sql(this);
    }

    public void run(String query, Object... params) {
        dbCommand("", query, params);
    }

    public Object dbCommand(String command, String query, Object... params) {
        try (PreparedStatement ps = getCurrentThreadConnection().prepareStatement(query, Statement.RETURN_GENERATED_KEYS)) {
            if (params != null) setPreparedStatementParameters(ps, params);

            switch (command) {
                case "SELECT" -> {
                    ResultSet rs = ps.executeQuery();
                    List<Map<String, Object>> resultList = new ArrayList<>();
                    ResultSetMetaData metaData = rs.getMetaData();
                    while (rs.next()) {
                        Map<String, Object> row = new HashMap<>();
                        mappingData(metaData, row, rs);
                        resultList.add(row);
                    }
                    return resultList;
                }
                case "INSERT" -> {
                    ps.executeUpdate();
                    ResultSet rs = ps.getGeneratedKeys();

                    if (rs.next()) return rs.getLong(1);
                    else throw new RuntimeException("No generated key returned.");
                }
                case "UPDATE" -> {
                    return ps.executeUpdate();
                }
                case "DELETE" -> {
                    return ps.executeUpdate();
                }
                default -> {
                    return ps.executeUpdate();
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {
        clearCurrentThreadConnection();
    }

    private void clearCurrentThreadConnection() {
        Connection connection = connections.get(Thread.currentThread().getName());
        if (connection == null) return;
        try {
            connection.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            connections.remove(Thread.currentThread().getName());
        }
    }

    public void startTransaction() {
        try {
            getCurrentThreadConnection().setAutoCommit(false);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void rollback() {
        try {
            getCurrentThreadConnection().rollback();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void commit() {
        try {
            getCurrentThreadConnection().commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static void setPreparedStatementParameters(PreparedStatement ps, Object[] params) throws SQLException {
        for (int i = 0; i < params.length; i++) {
            ps.setObject(i + 1, params[i]);
        }
    }

    private static void mappingData(ResultSetMetaData metaData, Map<String, Object> map, ResultSet rs) throws SQLException {
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            map.put(metaData.getColumnName(i), rs.getObject(i));
        }
    }
}
