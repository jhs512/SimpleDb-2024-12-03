package com.ll.simpleDb;

import java.sql.*;

public class SimpleDb extends Sql{

    SimpleDb(String url, String id, String pw, String dbName) {
        super(url, id, pw, dbName);
    }

    void run(String query) {
        Statement stmt = null;
        try {
            stmt = con.createStatement();
            stmt.execute(query);
            stmt.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    void setDevMode(boolean isDev){
        this.isDev = isDev;
    }


    void run(String query,String title,String body, boolean isBlind){
        try {
            PreparedStatement stmt = con.prepareStatement(query);
            stmt.setString(1, title); // 첫 번째 ?에 바인딩
            stmt.setString(2, body);  // 두 번째 ?에 바인딩
            stmt.setBoolean(3, isBlind); // 세 번째 ?에 바인딩
            stmt.executeUpdate();
            stmt.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }


    }
    Sql genSql(){
        return new Sql(url, id, pw, dbName);
    }


}
