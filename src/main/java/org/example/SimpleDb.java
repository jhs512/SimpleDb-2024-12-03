package org.example;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.logging.Logger;

@RequiredArgsConstructor
public class SimpleDb {

    private final String host;
    private final String user;
    private final String password;
    private final String dbName;
    private boolean onDev;
    private final ThreadLocal<Connection> connectionHolder = new ThreadLocal<>();


    public void setDevMode(boolean check) {
        onDev = check;
    }

    // 초기세팅
    public void run(String sql, Object... params) {

        connect();
        Connection conn = connectionHolder.get();

        try (PreparedStatement pst = conn.prepareStatement(sql)) {
            bindParameters(pst, params);
            pst.executeUpdate();

        } catch (SQLException e) {
            throw new RuntimeException("Failed to execute SQL : " + sql + " Error : " + e.getMessage(), e);
        }
    }

    public Sql genSql() {
        connect();
        Connection conn = connectionHolder.get();
        return new Sql(conn);

    }

    private void bindParameters(PreparedStatement pst, Object[] params) throws SQLException {
        for (int i = 0; i < params.length; i++) {
            pst.setObject(i + 1, params[i]);
        }
    }

    private void connect() {

        try {
            Connection conn = connectionHolder.get();
            // Conn 이 생성이 안됐걷나 닫혀있다면 새로 생성
            if (conn == null || conn.isClosed()) {
                String url = String.format("jdbc:mysql://%s/%s?useSSL=false&allowPublicKeyRetrieval=true", host, dbName);
                // 멀티스레드 환경에서 경합 상태 방지
                // 스레드 풀 도입하면 그걸로 대체 가능
                synchronized (this) {
                    conn = DriverManager.getConnection(url, user, password);
                    connectionHolder.set(conn);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to connect DB : ", e);
        }
    }


    public void startTransaction() {

        Connection conn = connectionHolder.get();

        // 없으면 conn 생성
        if (conn == null) {
            connect();
            conn = connectionHolder.get();
        }
        try {
            conn.setAutoCommit(false);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to start transaction", e);
        }
    }


    public void rollback() {

        Connection conn = connectionHolder.get();

        if (conn == null) {
            throw new RuntimeException("No Start Transaction");
        }

        try {
            conn.rollback();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to rollback", e);
        }
        // 자원 정리
        closeConnection();
    }

    public void closeConnection() {
        Connection conn = connectionHolder.get();
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                throw new RuntimeException("Failed to close connection", e);
            } finally {
                connectionHolder.remove();
            }
        }
    }

    public void commit() {

        Connection conn = connectionHolder.get();
        if(conn == null) {
            throw new RuntimeException("No Start Transaction");
        }

        try {
            conn.commit();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to commit", e);
        } finally {
            closeConnection();
        }

    }
}
