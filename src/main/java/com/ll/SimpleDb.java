package com.ll;


import lombok.Setter;

import javax.sql.DataSource;
import java.sql.*;


public class SimpleDb {
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

    public long run(String query, Object ... params){
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
        return new Sql(this);
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

    /*
     * devMode true 로 설정 시 생성된 sql문 출력
     *
     * @param ps Statement 객체
     * */
    private void loggingSql(Statement ps) {
        if (devMode) {
            System.out.println(ps);
        }
    }

    /*
     * 전달받은 PreparedStatement에 인자를 채우고 리스트를 초기화
     *
     * @param ps PreparedStatement
     * */
    private void addParams(PreparedStatement ps) throws SQLException {
        for (int i = 1; i <= params.size(); i++) {
            ps.setObject(i, params.get(i - 1));
        }
        params.clear();
    }

    public void closeConnection() {
        Connection conn = threadLocalConnection.get();
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                throw new RuntimeException("커넥션 연결 실패 : " + e.getMessage());
            } finally {
                threadLocalConnection.remove();
            }
        }
    }

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
