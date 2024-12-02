package com.ll.simpleDb;

import java.sql.*;

public class SimpleDb extends Sql{

    SimpleDb(String url, String id, String pw, String dbName) {
        super(url, id, pw, dbName);
    }

    void run(String query) {
        try {
            Statement stmt = con.createStatement();
            stmt.execute(query);
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
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }


    }
    Sql genSql(){
        return this;
    }


}
