package com.ll.simpleDb;

import java.sql.*;

public class SimpleDb {
    String url;
    String id;
    String pw;
    String dbName;
    String port;
    Connection con;
    boolean isDev;
    boolean autoCommit = true;

    SimpleDb(String url, String id, String pw, String dbName) {
        this.url = url;
        this.id = id;
        this.pw = pw;
        this.dbName = dbName;
        this.port = "3306";
        init();
    }

    void init() {
        try {
            con = DriverManager.getConnection("jdbc:mysql://" + url + ":" + port + "/" + dbName, id, pw);
            con.setAutoCommit(false);

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    void run(String query) {
        this.genSql().append(query).create();
    }

    void run(String query, String title, String body, boolean isBlind) {
        this.genSql().append(query, title, body, isBlind).insert();
    }

    void setDevMode(boolean isDev) {
        this.isDev = isDev;
    }

    void closeConnection() {
        try {
            con.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    void rollback() {
        try {
            con.rollback();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    void commit(){
        try {
            con.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    void startTransaction() {
        autoCommit = false;
    }

    Sql genSql() {
        try {
            if (con.isClosed())
                init();
            return new Sql(con, autoCommit);

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


}
