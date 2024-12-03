package org.example;

import lombok.Getter;
import lombok.Setter;

import java.sql.*;

public class SimpleDb {
    private final String host;
    private final String username;
    private final String password;
    private final String url;
    @Getter @Setter
    private Boolean devMode = false;
    @Getter
    private final Connection connection;
    @Getter @Setter
    private PreparedStatement preparedStatement = null;


    public SimpleDb(String host, String username, String password, String dbName) {
        int port = 3306;

        this.url = "jdbc:mysql://" + host + ":" + port + "/" + dbName;

        this.host = host;
        this.username = username;
        this.password = password;
        this.connection = createConnection();
    }

    public Connection createConnection() {
        try{
            return DriverManager.getConnection(url, username, password);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void run(String sql) {
        try{
            preparedStatement = connection.prepareStatement(sql);

            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            System.out.println("sql 오류 " + e);
        }
    }

    public void run(String sql, String title, String body, Boolean isBlind) {
        try{
            sql = sql.replace("title = ?", "title = \"" + title + "\"");
            sql = sql.replace("`body` = ?", "`body` = \"" + body + "\"");
            sql = sql.replace("isBlind = ?", "isBlind = " + isBlind);

            preparedStatement = connection.prepareStatement(sql);
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            System.out.println("sql 오류 " + e);
        }
    }

    public Sql genSql() {
        return new Sql(this);
    }

}
