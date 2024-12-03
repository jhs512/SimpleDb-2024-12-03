package com.ll;


import lombok.Setter;

import java.sql.*;


public class SimpleDb {
    private Connection conn;
    private final String host;
    private final String user;
    private final String password;
    private final String db;

    private final String jdbc_url;

    @Setter
    private boolean devMode;

    public SimpleDb(String host, String user, String password, String db) {
        this.host = host;
        this.user = user;
        this.password = password;
        this.db = db;

        this.jdbc_url = "jdbc:mysql://" + host + ":3306/" + db;

        try {
            conn = DriverManager.getConnection(jdbc_url, user, password);
        }catch(SQLException e){
            e.printStackTrace();
        }
    }

    public void run(String query){
        try {
            Statement stmt = conn.createStatement();

            if(devMode){
                System.out.println(stmt);
            }
            stmt.execute(query);
        }catch(SQLException e){
            e.printStackTrace();
        }
    }

    public void run(String query, String title, String body, boolean isBlind){
        try {
            PreparedStatement ps = conn.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
            // title
            ps.setString(1, title);
            // body
            ps.setString(2, body);
            // blind
            ps.setBoolean(3, isBlind);

            if(devMode){
                System.out.println(ps);
            }

            int id = ps.executeUpdate();

        }catch(SQLException e){
            e.printStackTrace();
        }
    }

    public void close(){
        if (conn != null){
            try{
                conn.close();
            }catch(SQLException e){
                e.printStackTrace();
            }
        }
    }

    public Sql genSql() {
        return new Sql(conn, this.devMode, this);
    }

    public Connection getConnection() throws SQLException {
        return DriverManager.getConnection(jdbc_url, user, password);
    }

    public void closeConnection() {
        if (conn != null){
            try{
                conn.close();
            }catch(SQLException e){
                e.printStackTrace();
            }
        }
    }

    public void startTransaction() {
        if (conn != null){
            try{
                conn.setAutoCommit(false);

            }catch(SQLException e1){
                try{
                    conn.rollback();
                }catch (SQLException e2){
                    e2.printStackTrace();
                }
            }
        }
    }

    public void rollback() {
        if(conn != null){
            try{
                conn.rollback();
            }catch(SQLException ex){
                ex.printStackTrace();
            }
        }
    }
}
