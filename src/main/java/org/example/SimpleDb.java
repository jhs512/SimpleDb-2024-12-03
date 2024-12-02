package org.example;

import java.sql.*;

public class SimpleDb {
    private final int port = 3306;
    private boolean devMode;
    private Connection conn;
    private Sql sql;

    public SimpleDb(String host, String user, String password, String name){
        String url = "jdbc:mysql://"+ host + ":" + port + "/" + name;

        try {
            conn = DriverManager.getConnection(url, user, password);
            sql = new Sql(conn);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void setDevMode(boolean isDevMode) {
        this.devMode = isDevMode;
    }

    public void run(String sql) {
        try {
            Statement stmt = conn.createStatement();
            stmt.executeUpdate(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void run(String sql, String title, String body, boolean isBlind) {
        try {
            PreparedStatement pstmt = conn.prepareStatement(sql);
            pstmt.setString(1, title);
            pstmt.setString(2, body);
            pstmt.setBoolean(3, isBlind);

            pstmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public Sql genSql() {
        return this.sql;
    }
}
