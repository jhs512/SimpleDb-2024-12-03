package com.ll;

import java.sql.*;
import java.util.List;

public class SimpleDb {
    private Connection connection;

    public SimpleDb(String host, String user, String password, String database) {
        try {
            String url = String.format("jdbc:mysql://%s:%d/%s", host, 3306, database);
            this.connection = DriverManager.getConnection(url, user, password);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void setDevMode(boolean b) {
        // TODO
    }

    private long execute(String sql, Object... params) {
        long targetRow = 0;
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
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

    private final static long INVALID_ID = -1;
    public long insert(String sql, List<Object> params) {
        try (PreparedStatement stmt = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            for (int i = 0; i < params.size(); i++) {
                stmt.setObject(i + 1, params.get(i));
            }
            stmt.executeUpdate();
            ResultSet rs = stmt.getGeneratedKeys();
            if (rs.next()) {
                return rs.getLong(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // 키 생성이 안된 경우 or insert 가 아닌 다른 쿼리가 들어오는 경우
        return INVALID_ID;
    }

    public long update(String string, List<Object> params) {
        return execute(string, params.toArray());
    }

    public void close() {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void rollback() {
    }

    public void startTransaction() {
    }

    public void closeConnection() {
    }

    public void commit() {
    }

    public Sql genSql() {
        Sql sql = new Sql(this);
        return sql;
    }

    public long delete(String string, List<Object> params) {
        return execute(string, params.toArray());
    }
}
