package com.ll.simpleDb;

import com.ll.simpleDb.Sql.SqlImpl;
import com.ll.simpleDb.Sql.SqlDevImpl;

import java.sql.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SimpleDbImpl implements SimpleDb {
    private final String url;
    private final String user;
    private final String password;
    private final String dbName;
    private final Connection conn;
    private Map<Integer, Connection> connectionMap = new ConcurrentHashMap<>();
    private Boolean isDevMode = false;

    SimpleDbImpl(String url, String user, String password, String dbName) {
        this.url = url;
        this.user = user;
        this.password = password;
        this.dbName = dbName;
        this.conn = connect();
    }

    //TODO : 예외처리 리팩토링
    private Connection connect() {
        String target = "jdbc:mysql://" + this.url + "/" + this.dbName;
        Connection conn1;

        //JDBC 드라이버 로드
        try {
            Class.forName("com.mysql.cj.jdbc.Driver"); // MySQL의 경우
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e.getMessage());
        }

        //SqlImpl Server Connection 로드
        try {
            conn1 = DriverManager.getConnection(target, this.user, this.password);
        } catch (SQLException e) {
            //DB 연결 실패 시 프로그램 종료 -> 런타임 예외 발생
            throw new RuntimeException(e.getMessage());
        }
        return conn1;
    }

    @Override
    public void setDevMode(Boolean isDevMode) {
        this.isDevMode = isDevMode;
    }

    @Override
    public long run(String query) {
        return this.run(query, "");
    }

    public long run(String query, Object...params) {
        SqlImpl sqlImpl = genSql();
        sqlImpl.append(query, params);
        long affected = sqlImpl.update();
        closeConnection(sqlImpl.id);
        return affected;
    }

    @Override
    public SqlImpl genSql() {
        /*
        Todo 커넥션 풀 구조를 도입하여 과다한 커넥션 생성 방지 및 커넥션 재활용
         */
        Connection conn = connect();
        if (isDevMode) {
            SqlImpl sqlImpl = new SqlDevImpl(conn);
            connectionMap.put(sqlImpl.id, conn);
            return sqlImpl;
        }
        SqlImpl sqlImpl = new SqlImpl(conn);
        connectionMap.put(sqlImpl.id, conn);
        return sqlImpl;
    }

    @Override
    public void closeConnection(int id) {
        try {
            Connection conn = connectionMap.get(id);
            conn.close();
            connectionMap.remove(id);
        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public void startTransaction(int id) {
        try {
            Connection conn = connectionMap.get(id);;
            conn.setAutoCommit(false);
        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public void rollback(int id) {
        try {
            Connection conn = connectionMap.get(id);
            conn.rollback();
        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public void commit(int id) {
        try {
            Connection conn = connectionMap.get(id);
            conn.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage());
        }
    }
}
