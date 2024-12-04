package com.ll.simpleDb;

import static com.ll.simpleDb.DBConnectionUtil.createConnection;
import static com.ll.simpleDb.DBConnectionUtil.isConnectionValid;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import lombok.Getter;

/**
 * packageName  : com.ll.simpleDb
 * fileName     : ConnectionPool
 * author       : Author
 * date         : 2024-12-03
 * description  :
 * ====================================================================================================
 * DATE           AUTHOR              NOTE
 * ----------------------------------------------------------------------------------------------------
 * 2024-12-03     Author              Initial creation.
 */
public class ConnectionPool {

    private static final long CONNECTION_WAIT_TIMEOUT = 5000;

    @Getter
    private final BlockingQueue<Connection> pool;
    private final String                    user;
    private final String                    password;
    private final String                    url;
    private final int                       maxPoolSize;

    public ConnectionPool(final String user, final String password, final String url, final int maxPoolSize) {
        this.user = user;
        this.password = password;
        this.url = url;
        this.maxPoolSize = maxPoolSize;
        this.pool = new ArrayBlockingQueue<>(maxPoolSize);
        initializePool();
    }

    private void initializePool() {
        pool.clear();
        for (int i = 0; i < maxPoolSize; i++)
            pool.offer(createConnection(url, user, password, maxPoolSize));
    }

    Connection acquireConnection(final boolean txMode) {
        try {
            Connection conn = pool.poll(CONNECTION_WAIT_TIMEOUT, TimeUnit.MILLISECONDS);
            if (conn == null)
                throw new RuntimeException("No available connection after timeout");
            if (!isConnectionValid(conn))
                conn = createConnection(url, user, password, maxPoolSize);
            conn.setAutoCommit(!txMode);
            return conn;
        } catch (InterruptedException e) {
            throw new RuntimeException("Thread interrupted while waiting for connection", e);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    void releaseConnection(final Connection conn) {
        if (conn != null && pool.size() < maxPoolSize) pool.offer(conn);
        else closeConnection(conn);
    }

    void closeConnection(final Connection conn) {
        DBConnectionUtil.closeConnection(conn);
    }

    void resetPool() {
        initializePool();
    }

    void closePool() {
        while (!pool.isEmpty()) closeConnection(pool.poll());
    }

}
