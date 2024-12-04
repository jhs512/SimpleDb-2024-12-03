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
    private Connection connection;
    private Connection connectionOfSql;
    private PreparedStatement preparedStatement;
    private Boolean autoCommit = true;


    public SimpleDb(String host, String username, String password, String dbName) {
        int port = 3306;

        this.url = "jdbc:mysql://" + host + ":" + port + "/" + dbName;
        this.host = host;
        this.username = username;
        this.password = password;
    }

    public Connection createConnection() {
        Connection conn = null;
        try{
            conn = DriverManager.getConnection(url, username, password);
            conn.setAutoCommit(autoCommit);
            return conn;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void run(String sql, Object... params) {
        try{
            if(connection == null || connection.isClosed()) connection = createConnection();
            preparedStatement = connection.prepareStatement(sql);

            for(int i =0; i<params.length; i++ ) {
                preparedStatement.setObject(i+1, params[i]);
            }

            preparedStatement.executeUpdate();

        } catch (SQLException e) {
            System.out.println("SimpleDb에서 sql 오류 " + e);
        } finally {
            try{
                preparedStatement.close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }

        }
    }

    public Sql genSql() {
        connectionOfSql = createConnection();
        return new Sql(devMode, connectionOfSql);
    }

    public void closeConnection() {
        try{
            this.connection.close();
        } catch (SQLException e) {throw new RuntimeException();}
    }

    public void startTransaction() {
        this.autoCommit = false;
    }

    public void rollback() {
        try{
            connectionOfSql.rollback();
            this.autoCommit = true;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void commit() {
        try{
            connectionOfSql.commit();
            this.autoCommit = true;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
