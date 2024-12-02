package com.ll.simpleDb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class Sql {
    StringBuilder sql = new StringBuilder();
    List<String> sqlContent = new ArrayList<>() {
    };
    String url;
    String id;
    String pw;
    String dbName;
    String port;
    Connection con;

    boolean isDev;
    Sql(String url,String id,String pw,String dbName){
        this.url = url;
        this.id = id;
        this.pw = pw;
        this.dbName = dbName;
        port = "3306";
        init();
    }
    void init() {
        try {
            con = DriverManager.getConnection("jdbc:mysql://"+url+":"+port+"/"+dbName,id,pw);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    Sql append(String query){
        sql.append(query);
        return this;
    }
    Sql append(String query,String content){
        sql.append(query);
        sqlContent.add(content);
        return this;
    }
    long insert(){
        long id = -1;
        try {
            System.out.println(sql.toString());
            PreparedStatement stmt = con.prepareStatement(sql.toString());
            int n = 1 ;
            for(String i : sqlContent){
                stmt.setString(n++, i);
            }
            id = stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return id;
    }

}
