package com.ll.simpleDb;

import lombok.RequiredArgsConstructor;

import java.sql.*;

@RequiredArgsConstructor
public class SimpleDb {
    private final String host;
    private final String username;
    private final String password;
    private final String dbName;
    private Connection connection;

    private static final ThreadLocal<Connection> threadLocalConnection = new ThreadLocal<>();

    // 데이터베이스 연결 초기화
    private Connection connect()  {
        Connection conn = threadLocalConnection.get();
        try {
            if (conn == null || conn.isClosed()) {
                // 새 Connection 생성 및 저장
                String url = String.format("jdbc:mysql://%s/%s?useSSL=false", host, dbName);
                conn = DriverManager.getConnection(url, username, password);
                threadLocalConnection.set(conn);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return conn;
    }

    public Sql genSql() {
        connection = connect();
        return new Sql(connection);
    }

    public void closeConnection()  {
        Connection conn = threadLocalConnection.get();
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            threadLocalConnection.remove(); // 스레드에서 제거
        }
    }

    // SQL 실행 메서드
    public void run(String sql) {

        connection = connect(); // 연결 초기화

        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to execute SQL: " + sql, e);
        }
    }

    public void run(String sql, String title, String body, boolean isBlind) {
        connection = connect();// 연결 초기화
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setString(1, title);
            pstmt.setString(2, body);
            pstmt.setBoolean(3, isBlind);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to execute SQL: " + sql, e);
        }
    }

    public void setDevMode(boolean b) {
    }


    public void startTransaction() {
        try {
            connection.setAutoCommit(false);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void rollback() {
        try {
            connection.rollback();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void commit() {
        try {
            connection.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}