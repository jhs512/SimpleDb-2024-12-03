package com.ll.simpleDb;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

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
    Map<String,String> getColumnName(PreparedStatement stmt ) throws SQLException {
        Map<String,String> result = new HashMap<>();
        ResultSetMetaData metaData = stmt.getMetaData();
        int count = metaData.getColumnCount();
        for(;count >0; count--) {
            result.put(metaData.getColumnName(count),metaData.getColumnTypeName(count));
        }

        return result;
    }
    Object getColumnContent(ResultSet rs,String name,String type) throws SQLException {
            if(type.contains("INT"))
                return rs.getLong(name);
            if(type.equals("DATETIME") || type.equals("NOW()") ) {
                return rs.getTimestamp(name).toLocalDateTime();
            }
            if(type.contains("VARCHAR") || type.equals("TEXT"))
                return rs.getString(name);
            if(type.contains("BIT"))
                return rs.getBoolean(name);
            return null;

    }
    Object select(){
        List<Map<String, Object>> result = new ArrayList<>();
        try (PreparedStatement stmt = con.prepareStatement(sql.toString())) {
            //결과를 담을 ResultSet 생성 후 결과 담기
            ResultSet rs = stmt.executeQuery();
            Map<String,String> map = getColumnName(stmt);
            while(rs.next()) {
                Map<String,Object> save = new HashMap<>();
                for(String name : map.keySet()){
                    save.put(name,getColumnContent(rs,name, map.get(name)));

                }
                result.add(save);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        if(result.size() == 1) return result.getFirst();
        return result;
    }

    List<Map<String, Object>> selectRows(){
        return (List<Map<String, Object>>)select();
    }
    Map<String, Object> selectRow(){
        return (Map<String, Object>)select();
    }
    LocalDateTime selectDatetime(){
        String name = "";
        for(String i : selectRow().keySet())
            name = i;
        return (LocalDateTime) selectRow().get(name);
    }


}
