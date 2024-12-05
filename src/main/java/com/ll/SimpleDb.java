package com.ll;


import lombok.Setter;

import javax.sql.DataSource;
import javax.xml.transform.Result;
import java.sql.*;
import java.util.Arrays;
import java.util.List;


public class SimpleDb {
    private final String host;
    private String user;
    private String password;
    private final String db;
    private String jdbcUrl;

    @Setter
    private boolean devMode;

    public SimpleDb(String host, String user, String password, String db) {
        this.host = host;
        this.user = user;
        this.password = password;
        this.db = db;
        this.jdbcUrl = "jdbc:mysql://" + host + ":3306/" + db;
    }

    private <T> T executeUpdate(PreparedStatement ps, Class<T> cls) throws SQLException {
        int afftedRows = ps.executeUpdate();
        ResultSet generatedKeys = ps.getGeneratedKeys();


        if (generatedKeys.next() && cls == Long.class) {
            return (T) (Long) generatedKeys.getLong(1);
        } else {
            return (T) (Integer) ps.getUpdateCount();
        }
    }

    // 내부 SQL 실행 메서드
    private <T> T _run(String sql, Class<T> cls, Object... params) {
        Connection conn = getConnection();
        sql = sql.trim();

        try (PreparedStatement preparedStatement = conn.prepareStatement(sql, PreparedStatement.RETURN_GENERATED_KEYS)) {
            bindParams(preparedStatement, params);
            loggingSql(preparedStatement);
            if (sql.startsWith("INSERT")) {
                return executeUpdate(preparedStatement, cls);
            }
            else if (sql.startsWith("SELECT")) {
                try (ResultSet rs = preparedStatement.executeQuery()) {
                    return null;
                }
            }
            return (T) (Integer) preparedStatement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("SQL 실행 실패 : " + e.getMessage(), e);
        }
    }

    /*
    * sql 문 실행 후 반환 값이 int 라 long 사용 X
    * */
    public int run(String sql, Object ... params) {
        return _run(sql, Integer.class, params);
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
     * @param ps        PreparedStatement
     * @param params    Object[]
     * */
    private void bindParams(PreparedStatement ps, Object[] params) throws SQLException {
        // Column 은 1부터, 배열은 0부터 시작
        for (int i = 1; i <= params.length; i++) {
            ps.setObject(i, params[i-1]);
        }
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

    public long insert(String sql, Object[] params) {
        return _run(sql, Long.class, params);
    }

    public int update(String sql, Object[] params) {
        return _run(sql, Integer.class, params);
    }
}
