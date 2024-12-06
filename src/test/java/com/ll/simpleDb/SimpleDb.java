package com.ll.simpleDb;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

import static java.lang.Thread.currentThread;
import static java.lang.Thread.sleep;

public class SimpleDb {
    String url;
    String id;
    String pw;
    String dbName;
    String port;
    boolean isDev;
    boolean autoCommit = true;
    static int multiThreadCount = 0;
    Map<Thread, Connection> threadMap = new HashMap<>();
    private static final ThreadLocal<Connection> localCon = new ThreadLocal<>();

    SimpleDb(String url, String id, String pw, String dbName) {
        this.url = url;
        this.id = id;
        this.pw = pw;
        this.dbName = dbName;
        this.port = "3306";
    }

    Connection getConnection() {
        try {
            return DriverManager.getConnection("jdbc:mysql://" + url + ":" + port + "/" + dbName, id, pw);

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    void setLocalThread() {
        if (localCon.get() == null) {
            localCon.set(getConnection());
        }
        try {
            if (localCon.get().isClosed()){
                localCon.set(getConnection());
            }
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
            setLocalThread();
            localCon.get().close();
            multiThreadCount++;
            localCon.remove();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    void rollback() {
        try {
            setLocalThread();
            localCon.get().rollback();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    void commit() {
        try {
            setLocalThread();
            localCon.get().commit();
            //localCon.get().setAutoCommit(true);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    void startTransaction() {
        try {
            localCon.get().setAutoCommit(false);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    void stayForMultiThread() throws InterruptedException {
        String name = currentThread().getName();
        System.out.println(name);
        String a = name.substring(name.length() - 1);

        char b = a.toCharArray()[0];
        if (Character.isDigit(b)) {
            while (multiThreadCount != Integer.parseInt(a)) {
                Thread.sleep(10);
            }
        }
    }

    Sql genSql() {

        setLocalThread();
        return new Sql(localCon.get(), autoCommit, isDev);

    }


}
