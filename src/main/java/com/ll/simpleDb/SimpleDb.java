package com.ll.simpleDb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

public class SimpleDb {

    private static final ThreadLocal<Connection> threadLocalConnection = ThreadLocal.withInitial(() -> {
        try {
            String url = "jdbc:mysql://localhost:3307/simpleDb__test";
            return DriverManager.getConnection(url, "root", "lldj123414");
        } catch (SQLException e) {
            throw new RuntimeException("Error connecting to database", e);
        }
    });

    public SimpleDb(String ip, String user, String password, String databaseName) {

    }

    private Connection getConnection() {
        return threadLocalConnection.get();
    }

    public void setDevMode(boolean state) {

    }

    public void run(String query) {
        try {
            Connection connection = getConnection();
            Statement statement = connection.createStatement();
            statement.executeUpdate(query);

            System.out.println("Query executed successfully");
            statement.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void run(String query, String title, String body, boolean isBlind) {
        try {
            Connection connection = getConnection();
            PreparedStatement statement = connection.prepareStatement(query);
            statement.setString(1, title);
            statement.setString(2, body);
            statement.setBoolean(3, isBlind);

            int result = statement.executeUpdate();
            if (result > 0) {
                System.out.println("Create Article success");
            } else {
                System.out.println("Create Article failed");
            }

            statement.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void closeConnection() {
        try {
            Connection connection = getConnection();
            if(connection != null) {
                connection.close();
                threadLocalConnection.remove();
                System.out.println("Connection closed");
            }
        } catch (SQLException e) {
            System.err.println("Error closing connection");
        }
    }

    public Sql genSql() {
        return new Sql(getConnection());
    }
}

