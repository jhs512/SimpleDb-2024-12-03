package com.ll.simpleDb;

import com.ll.simpleDb.Sql.SqlImpl;
import com.ll.simpleDb.Sql.SqlDevImpl;

import java.sql.*;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.IntStream;

public class SimpleDbImpl implements SimpleDb {
    private final String url;
    private final String user;
    private final String password;
    private final String dbName;
    private final Connection conn;
    private Map<UUID, Connection> connectionMap = new ConcurrentHashMap<>();
    private ThreadLocal<Connection> connectionThreadLocal = new ThreadLocal<>();
    private Boolean isDevMode = false;
    // 커넥션 풀
    private static Queue<Connection> connectionPool = new ConcurrentLinkedQueue<>();

    SimpleDbImpl(String url, String user, String password, String dbName, int maxPool) {
        this.url = url;
        this.user = user;
        this.password = password;
        this.dbName = dbName;
        this.conn = connect();

        //커넥션 풀에 커넥션 생성
        IntStream.range(0,maxPool).forEach(
            it -> connectionPool.add(connect())
        );
    }

    SimpleDbImpl(String url, String user, String password, String dbName) {
        this(url, user, password, dbName, 8);
    }

    //커넥션 풀에서 커넥션 가져오기
    private synchronized Connection getConn() {
        Connection conn = connectionPool.poll();
        while (conn == null) {
            try {
                wait();
                conn = connectionPool.poll();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        return conn;
    }


    /**
     * @param id
     * @apiNote 사용했던 커넥션 풀을 반납합니다
     * genSql 메서드 사용 시 꼭 !!!!!!! 명시적으로 반납해야합니다
     */
    public synchronized void returnConn(UUID id) {
        Connection conn = connectionMap.get(id);
        connectionPool.add(conn);
        connectionMap.remove(id);
        notifyAll();
    }

    public synchronized void returnConn() {
        Connection conn = connectionThreadLocal.get();
        if (conn != null) {
            connectionPool.add(conn);
            connectionThreadLocal.remove();
            notifyAll();
        }

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

    /**
     * @param isDevMode
     * @apiNote 개발자 모드를 사용 시 쿼리 실행 전 터미널에 쿼리를 출력하는 객체
     * SqlDevImpl를 SqlImpl 대신 반환합니다
     */
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
        long result = sqlImpl.update();
        returnConn();
        return result;
    }

    @Override
    public SqlImpl genSql() {
        /*
        Todo 커넥션 풀 구조를 도입하여 과다한 커넥션 생성 방지 및 커넥션 재활용
        커넥션 풀 개발 완료하였습니다
         */
        Connection conn = getConn();
        if (isDevMode) {
            SqlImpl sqlImpl = new SqlDevImpl(conn);
            connectionThreadLocal.set(conn);
            return sqlImpl;
        }
        SqlImpl sqlImpl = new SqlImpl(conn);
        connectionThreadLocal.set(conn);
        return sqlImpl;
    }

    @Override
    public void closeConnectionAll() {
        connectionPool.forEach(
            conn-> {
                try {
                    conn.close();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        );
    }
    @Override
    public void closeConnection(UUID id) {
        try {
            Connection conn = connectionMap.get(id);
            conn.close();
            connectionMap.remove(id);
            connectionPool.add(connect());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized void closeConnection() {
        try {
            Connection conn = connectionThreadLocal.get();
            if (conn != null) {
                conn.close();
                connectionThreadLocal.remove();
                connectionPool.add(connect());
                notifyAll();
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    @Override
    public void startTransaction(UUID id) {
        try {
            Connection conn = connectionMap.get(id);
            conn.setAutoCommit(false);
        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    public void startTransaction() {
        try {
            Connection conn = connectionThreadLocal.get();
            conn.setAutoCommit(false);
        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public void rollback(UUID id) {
        try {
            Connection conn = connectionMap.get(id);
            conn.rollback();
        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    public void rollback() {
        try {
            Connection conn = connectionThreadLocal.get();
            conn.rollback();
        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public void commit(UUID id) {
        try {
            Connection conn = connectionMap.get(id);
            conn.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    public void commit() {
        try {
            Connection conn = connectionThreadLocal.get();
            conn.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage());
        }
    }
}
