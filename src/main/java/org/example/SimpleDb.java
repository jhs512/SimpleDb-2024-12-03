package org.example;

import lombok.Getter;
import lombok.Setter;

import java.sql.*;

@Getter
@Setter
public class SimpleDb {
    private final String host;

    private final String username;

    private final String password;

    private String dbName;

    private boolean devMode;

    public SimpleDb(String host, String username, String password, String dbName) {
        this.host = "jdbc:mysql://" + host +"/" + dbName;
        this.username = username;
        this.password = password;
        this.dbName = dbName;
        this.devMode = false;
    }



    public void run(String expr, Object... params) {
        try{
            Connection connection = DriverManager.getConnection(host, username, password);
            PreparedStatement statement = connection.prepareStatement(expr);

            for(int i=0; i<params.length; i++){
                statement.setObject(i+1, params[i]);
            }
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public Sql genSql() {
        return new Sql(host, username, password, devMode);
    }
}
