package com.ll;

import lombok.Setter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

@Setter
public class SimpleDb {

    private final String url;
    private final String username;
    private final String password;
    private final String dbName;
    private final String dbUrl;
    private boolean devMode;
    private static ThreadLocal<List<String>> threadLocalList;


    public SimpleDb(String url, String username, String password, String dbName) {
        this.url = url;
        this.username = username;
        this.password = password;
        this.dbName = dbName;
        dbUrl = "jdbc:mysql://" + url + "/" + dbName;
    }

    public Sql genSql() {
        return new Sql(dbUrl, username, password);
    }

    public void run(String sql, Object... args) {

        Connection connection = null;
        PreparedStatement statement = null;


        try {
            connection = DriverManager.getConnection(dbUrl, username, password);
            statement = connection.prepareStatement(sql);

            for (int i=0; i < args.length; ++i) {
                statement.setObject(i+1, args[i]);
            }

            statement.executeUpdate();


        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                };
            }

            if (statement != null) {
                try {
                    connection.close();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                };
            }
        }
    }


    public void closeConnection() {
    }
}
