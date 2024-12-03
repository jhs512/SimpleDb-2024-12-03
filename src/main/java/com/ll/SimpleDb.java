package com.ll;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class SimpleDb {
    private Connection connection;

    public SimpleDb(String host, String user, String password, String database) {
        try {
            String url = String.format("jdbc:mysql://%s:%d/%s", host, 3306, database);
            this.connection = DriverManager.getConnection(url, user, password);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void setDevMode(boolean b) {

    }

    public void run(String sql, Object... params) {
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            for (int i = 0; i < params.length; i++) {
                stmt.setObject(i + 1, params[i]);
            }
            stmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void close() {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void rollback() {
    }

    public void startTransaction() {
    }

    public void closeConnection() {
    }

    public void commit() {
    }

}
