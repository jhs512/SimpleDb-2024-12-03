package com.ll.simpleDb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

public class SimpleDb {

    // MySQL 연결 객체
    private static Connection connection = null;

    public SimpleDb(String ip, String user, String password, String databaseName) {
        // MySQL 연결 URL
        String url = "jdbc:mysql://" + ip + ":3307/" + databaseName;

        try {
            // MySQL JDBC 드라이버 로딩
            Class.forName("com.mysql.cj.jdbc.Driver");
            // 데이터베이스 연결
            connection = DriverManager.getConnection(url, user, password);

            if(connection != null) {
                System.err.println("Connected to database");
            }
        } catch (ClassNotFoundException e) {
            System.err.println("JDBC Driver not found");
            throw new RuntimeException(e);
        } catch (SQLException e) {
            System.err.println("Error connecting to database");
            throw new RuntimeException(e);
        }

    }

    public void setDevMode(boolean b) {

    }

    public void run(String query) {
        try {
            Statement statement = connection.createStatement();
            statement.executeUpdate(query);

            System.out.println("run success");
            statement.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    public void run(String query, String title, String body, boolean isBlind) {
        try {
            PreparedStatement statement = connection.prepareStatement(query);
            statement.setString(1, title);
            statement.setString(2, body);
            statement.setBoolean(3, isBlind);

            statement.executeUpdate();
            System.out.println("Create Article success");
            statement.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {
        try {
            if(connection != null) {
                connection.close();
                System.out.println("Connection closed");
            }
        } catch (SQLException e) {
            System.err.println("Error closing connection");
        }
    }

}
