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

    public void run(String query) {
        sql.run(query);
    }

    public void run(String query, String title, String body, boolean isBlind) {
        String newSql = query.replace("title = ?", "title = " + "'" + title + "'")
                .replace("`body` = ?", "body = " + "'" + body + "'")
                .replace("isBlind = ?", "isBlind = " + isBlind);
        sql.run(newSql);
    }

    public Sql genSql() {
        return this.sql;
    }
}
