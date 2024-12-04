package com.ll;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class SimpleDb {
    private Connection conn;
    private String host;
    private String user;
    private String password;
    private String database;
    private boolean autoCommit = true;
    private List<Sql> connections = new ArrayList<>();

    public SimpleDb(String host, String user, String password, String database) {
        this.host = host;
        this.user = user;
        this.password = password;
        this.database = database;
        conn = getConn();
    }

    public synchronized Connection getConn(){
        Connection conn = null;
        try {
            String url = String.format("jdbc:mysql://%s:%d/%s", host, 3306, database);
            conn = DriverManager.getConnection(url, user, password);
            conn.setAutoCommit(autoCommit);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }

    public void setDevMode(boolean b) {
        // TODO
    }

    private long execute(String sql, Object... params) {
        long targetRow = 0;
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
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

    public void close() {
        try {
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void closeConnection() {
        try{
            conn.close();
            connections.forEach(sql -> {
                try {
                    sql.close();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            });
        }catch (SQLException e){
            e.printStackTrace();
        }
    }

    public Sql genSql() {
        Sql sql = new SqlImpl(getConn());
        return sql;
    }

}
