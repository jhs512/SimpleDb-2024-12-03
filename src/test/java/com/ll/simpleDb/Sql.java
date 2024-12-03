package com.ll.simpleDb;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    long runQeury(){
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
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return id;
    }
    long insert(){
        return runQeury();
    }

    long update(){

        return runQeury();
    }

    long delete(){
        return runQeury();
    }

    List<Map<String, Object>> selectRows(){
        List<Map<String, Object>> result = new ArrayList<>();
        try (PreparedStatement stmt = con.prepareStatement(sql.toString())) {
            //결과를 담을 ResultSet 생성 후 결과 담기
            ResultSet rs = stmt.executeQuery();
            while(rs.next()) {
                Map<String,Object> save = new HashMap<>();
                save.put("id",rs.getLong("id"));
                save.put("title",rs.getString("title"));
                save.put("body",rs.getString("body"));
                save.put("createdDate",convertDateToLocalDataTime(rs.getDate("createdDate")));
                save.put("modifiedDate",convertDateToLocalDataTime(rs.getDate("modifiedDate")));
                save.put("isBlind",rs.getBoolean("isBlind"));
                result.add(save);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    LocalDateTime convertDateToLocalDataTime(Date d){
        return new java.sql.Timestamp(d.getTime()).toLocalDateTime();
    }


}
