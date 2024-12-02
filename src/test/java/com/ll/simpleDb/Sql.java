package com.ll.simpleDb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class Sql {
    StringBuilder sql = new StringBuilder();
    List<String> sqlContent = new ArrayList<>() {};
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
        sql.append(query).append(" ");
        return this;
    }
    Sql append(String query,String content){
        sql.append(query).append(" ");
        sqlContent.add(content);
        return this;
    }
    Sql append(String query,int...contents){
        sql.append(query).append(" ");
        for(int content : contents)
            sqlContent.add(content+"");
        return this;
    }
    long insert(){
        long id = -1;
        try {
            PreparedStatement stmt = con.prepareStatement(sql.toString());
            int n = 1 ;
            for(String i : sqlContent){
                stmt.setString(n++, i);
            }
            id = stmt.executeUpdate();
            stmt.close();
            con.close();
            init();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return id;
    }

    long update(){
        long id = -1;
        try {
            PreparedStatement stmt = con.prepareStatement(sql.toString());
            int n = 1 ;
            for(String i : sqlContent){
                if(i.chars().allMatch(Character::isDigit))
                    stmt.setInt(n++, Integer.parseInt(i));
                else
                    stmt.setString(n++,i);
            }
            id = stmt.executeUpdate();
            stmt.close();
            con.close();
            init();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return id;
    }

}
