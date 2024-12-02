package org.example;

import lombok.Getter;
import lombok.Setter;

import java.sql.*;

@Getter
@Setter
public class SimpleDb {
    private final String host;

    private final String username;

    private final String password;

    private String dbName;

    private boolean devMode;

    private ThreadLocal<Connection> connectionThreadLocal = ThreadLocal.withInitial(() -> null);

    public SimpleDb(String host, String username, String password, String dbName) {
        this.host = "jdbc:mysql://" + host + "/" + dbName;
        this.username = username;
        this.password = password;
        this.dbName = dbName;
        this.devMode = false;
    }

    private Connection getConnection(){
        // 현재 스레드의 connection을 반환
        try {
            Connection connection = connectionThreadLocal.get();
            if (connection == null || connection.isClosed()) {
                connection = DriverManager.getConnection(host, username, password);
            }
            connectionThreadLocal.set(connection);
            return connection;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void run(String expr, Object... params) {
        try {
            Connection connection = getConnection();
            PreparedStatement statement = connection.prepareStatement(expr);
            for (int i = 0; i < params.length; i++) {
                statement.setObject(i + 1, params[i]);
            }
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public Sql genSql() {
        Connection connection = getConnection();
        return new Sql(connection, devMode);
    }
    public void closeConnection() {
        try {
            Connection connection = getConnection();
            if (connection != null) {
                connection.close();
                connectionThreadLocal.remove();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void startTransaction() {
        try {
            Connection connection = getConnection();
            connection.setAutoCommit(false);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void rollback() {
        try {
            Connection connection = getConnection();
            if (connection != null && !connection.getAutoCommit()) {
                connection.rollback();
                connection.setAutoCommit(true);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void commit() {
        try {
            Connection connection = getConnection();
            if (connection != null && !connection.getAutoCommit()) {
                connection.commit();
                connection.setAutoCommit(true);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
