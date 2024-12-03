package com.ll;


import lombok.Setter;

import java.sql.*;


public class SimpleDb {
    private Connection conn;

    @Setter
    private boolean devMode;

    public SimpleDb(String host, String user, String password, String db) {
        String jdbc_url = "jdbc:mysql://" + host + ":3306/" + db;

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

    public Sql genSql() {
        return new Sql(conn, this.devMode);
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
}
