package com.ll;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.*;

public class SimpleDb {
    private HikariDataSource dataSource = null;

    public SimpleDb(String host, String user, String password, String database) {
        HikariConfig config = new HikariConfig();

        String jdbcUrl = String.format("jdbc:mysql://%s:3306/%s", host, database);
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(user);
        config.setPassword(password);

        // 기본 설정들
        config.setMaximumPoolSize(10);
        config.setMinimumIdle(5);
        config.setIdleTimeout(300000); // 5분
        config.setConnectionTimeout(10000); // 10초
        config.setPoolName("SimpleDbPool");

        this.dataSource = new HikariDataSource(config);
    }

    public Connection getConn() throws SQLException {
        return dataSource.getConnection();
    }

    public void setDevMode(boolean b) {
        // TODO
    }

    private long execute(String sql, Object... params) {
        long targetRow = 0;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            for (int i = 0; i < params.length; i++) {
                stmt.setObject(i + 1, params[i]);
            }
            targetRow = stmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return targetRow;
    }

    public void run(String sql, Object... params) {
        execute(sql, params);
    }

    public Sql genSql() {
        Sql sql = new SqlImpl(dataSource);
        return sql;
    }

    public void close() {
        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
        }
    }
}
