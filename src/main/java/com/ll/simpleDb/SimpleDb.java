package com.ll.simpleDb;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/**
 * packageName  : com.ll.simpleDb
 * fileName     : SimpleDb
 * author       : Author
 * date         : 2024-12-03
 * description  :
 * ====================================================================================================
 * DATE           AUTHOR              NOTE
 * ----------------------------------------------------------------------------------------------------
 * 2024-12-03     Author              Initial creation.
 */
public class SimpleDb {

    private static final int MAX_POOL_SIZE = 10;

    private final ConnectionPool connectionPool;

    private boolean txMode;

    public SimpleDb(final String host, final int port, final String user, final String password, final String dbName) {
        String url = "jdbc:mariadb://%s:%d/%s".formatted(host, port, dbName);
        connectionPool = new ConnectionPool(user, password, url, MAX_POOL_SIZE);
        txMode = false;
    }

    public void closePool() {
        connectionPool.closePool();
    }

    public void resetPool() {
        connectionPool.resetPool();
    }

    public void run(final String sql, final Object... params) {
        Connection conn = connectionPool.acquireConnection(txMode);

        List<Object> sqlParams = Arrays.asList(params);

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            Sql.setParams(pstmt, sqlParams);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to execute sql: " + sql, e);
        } finally {
            connectionPool.releaseConnection(conn);
        }
    }

    public Sql genSql() {
        Connection conn = connectionPool.acquireConnection(txMode);
        return new Sql(conn, () -> connectionPool.releaseConnection(conn));
    }

    public void closeConnection() {
        connectionPool.getPool().stream().filter(conn -> {
            try {
                return !conn.isClosed() && conn.isValid(0);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }).forEach(connectionPool::closeConnection);
    }

    public void startTransaction() {
        connectionPool.getPool().stream().filter(conn -> {
            try {
                return !conn.isClosed() && conn.getAutoCommit();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }).forEach(conn -> {
            try {
                conn.setAutoCommit(false);
                if (!conn.getAutoCommit()) txMode = true;
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public void commit() {
        connectionPool.getPool().stream().filter(conn -> {
            try {
                return !conn.isClosed() && !conn.getAutoCommit();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }).forEach(conn -> {
            try {
                conn.commit();
                conn.setAutoCommit(true);
                if (conn.getAutoCommit()) txMode = false;
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public void rollback() {
        connectionPool.getPool().stream().filter(conn -> {
            try {
                return !conn.isClosed() && !conn.getAutoCommit();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }).forEach(conn -> {
            try {
                conn.rollback();
                conn.setAutoCommit(true);
                if (conn.getAutoCommit()) txMode = false;
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }

}
