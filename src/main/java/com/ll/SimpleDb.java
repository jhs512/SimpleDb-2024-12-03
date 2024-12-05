package com.ll;


import lombok.Setter;

import java.sql.*;


public class SimpleDb implements DataSource {
    private final String host;
    private String user;
    private String password;
    private final String db;
    private String jdbcUrl;
    private Connection conn;
    @Setter
    private boolean devMode;

    public SimpleDb(String host, String user, String password, String db) {
        this.host = host;
        this.user = user;
        this.password = password;
        this.db = db;
        this.jdbcUrl = "jdbc:mysql://" + host + ":3306/" + db;

        try {
            conn = DriverManager.getConnection(jdbcUrl, user, password);
        }catch(SQLException e){
            throw new RuntimeException(e.getMessage());
        }
    }

    public void run(String query){
        try {
            Statement stmt = conn.createStatement();

            if(devMode){
                System.out.println(stmt);
            }
            stmt.execute(query);
        }catch(SQLException e){
            throw new RuntimeException(e.getMessage());
        }
    }

    public void run(String query, String title, String body, boolean isBlind){
        try {
            PreparedStatement ps = conn.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
            // title
            ps.setString(1, title);
            // body
            ps.setString(2, body);
            // blind
            ps.setBoolean(3, isBlind);

            if(devMode){
                System.out.println(ps);
            }

            int id = ps.executeUpdate();

        }catch(SQLException e){
            throw new RuntimeException(e.getMessage());
        }
    }

    public Sql genSql() {
        return new Sql(getConnection(), this.devMode);
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

    public void closeConnection() {
        Connection conn = threadLocalConnection.get();
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                throw new RuntimeException("커넥션 연결 실패 : " + e.getMessage(), e);
            } finally {
                threadLocalConnection.remove();
            }
        }
    }


    @Override
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

    @Override
    public void rollback() {
        Connection conn = threadLocalConnection.get();
        if(conn != null){
            try{
                conn.rollback();
                conn.setAutoCommit(true);
            }catch(SQLException e){
                throw new RuntimeException("롤백 실패 : " + e);
            }
        }
    }

    @Override
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
}
