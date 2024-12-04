package com.ll.simpleDb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * packageName  : com.ll.simpleDb
 * fileName     : DBConnectionUtil
 * author       : Author
 * date         : 2024-12-03
 * description  :
 * ====================================================================================================
 * DATE           AUTHOR              NOTE
 * ----------------------------------------------------------------------------------------------------
 * 2024-12-03     Author              Initial creation.
 */
public class DBConnectionUtil {

    private static final String TIME_ZONE = "Asia/Seoul";

    static Connection createConnection(
            final String url, final String user, final String password, final int maxPoolSize
    ) {
        try {
            Properties properties = new Properties();
            properties.setProperty("user", user);
            properties.setProperty("password", password);
            properties.setProperty("maxPoolSize", String.valueOf(maxPoolSize));
            properties.setProperty("serverTimezone", TIME_ZONE);

            return DriverManager.getConnection(url, properties);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to create connection", e);
        }
    }

    static void closeConnection(final Connection conn) {
        if (conn != null)
            try {
                if (!conn.isClosed()) conn.close();
            } catch (SQLException e) {
                throw new RuntimeException("Failed to close connection", e);
            }
    }

    static boolean isConnectionValid(final Connection conn) {
        try {
            return conn != null && !conn.isClosed() && conn.isValid(0);
        } catch (SQLException e) {
//            e.printStackTrace();
            return false;
        }
    }

}
