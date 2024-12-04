package com.ll.simpleDb;

import java.security.Key;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Sql {
    StringBuilder sql = new StringBuilder();
    String url;
    String id;
    String pw;
    String dbName;
    String port;
    Connection con;

    boolean isDev;

    Sql(String url, String id, String pw, String dbName) {
        this.url = url;
        this.id = id;
        this.pw = pw;
        this.dbName = dbName;
        port = "3306";
        init();
    }

    void init() {
        try {
            con = DriverManager.getConnection("jdbc:mysql://" + url + ":" + port + "/" + dbName, id, pw);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    Sql append(String query) {
        sql.append(query).append(" ");
        return this;
    }

    Sql append(String query, String content) {
        query = query.replaceFirst("\\?","'"+content+"'");
        sql.append(query).append(" ");
        return this;
    }
    Sql append(String query, int... contents) {
        for (int content : contents)
            query = query.replaceFirst("\\?",content+"");
        sql.append(query).append(" ");
        return this;
    }

    Sql appendIn(String query, int... contents) {
        StringBuilder save = new StringBuilder();
        for(int i = 0; i< contents.length; i++) {
            save.append(contents[i]);
            if(i <contents.length-1) save.append(", ");
        }
        query = query.replaceFirst("\\?",save.toString());
        sql.append(query).append(" ");
        return this;
    }
    Sql appendIn(String query, Long[] contents) {
        StringBuilder save = new StringBuilder();
        for (int i = 0; i < contents.length; i++) {
            save.append(contents[i]);
            if (i < contents.length - 1) save.append(", ");
        }
        query = query.replaceFirst("\\?", save.toString());
        sql.append(query).append(" ");
        return this;
    }

    long runQeury() {
        long id = -1;
        try {
            PreparedStatement stmt = con.prepareStatement(sql.toString());
            id = stmt.executeUpdate();
            stmt.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return id;
    }

    long insert() {
        return runQeury();
    }

    long update() {

        return runQeury();
    }

    long delete() {
        return runQeury();
    }

    Map<String, String> getColumnName(ResultSet rs) throws SQLException {
        Map<String, String> result = new HashMap<>();
        ResultSetMetaData metaData = rs.getMetaData();
        int count = metaData.getColumnCount();
        for (; count > 0; count--) {
            result.put(metaData.getColumnName(count), metaData.getColumnTypeName(count));
        }

        return result;
    }
    boolean isQueryBoolean(String type,String name){
        if (type.chars().allMatch(Character::isDigit)) {
            return (Integer.parseInt(name) == 0) || (Integer.parseInt(name) == 1);
        }
        return name.equals("1 = 0") || name.equals("1 = 1") || type.contains("BIT");
    }
    Object getColumnContent(ResultSet rs, String name, String type) throws SQLException {
        if (isQueryBoolean(type,name)) {
                return rs.getBoolean(name);
        }

        if (type.contains("INT"))
            return rs.getLong(name);
        if (type.equals("DATETIME")) {
            return rs.getTimestamp(name).toLocalDateTime();
        }
        if (type.contains("VARCHAR") || type.equals("TEXT"))
            return rs.getString(name);

        return null;

    }

    Object select() {
        List<Map<String, Object>> result = new ArrayList<>();
        try {
            PreparedStatement stmt = con.prepareStatement(sql.toString());
            ResultSet rs = stmt.executeQuery();

            Map<String, String> map = getColumnName(rs);
            while (rs.next()) {
                Map<String, Object> save = new HashMap<>();
                for (String name : map.keySet()) {
                    save.put(name, getColumnContent(rs, name, map.get(name)));
                }
                result.add(save);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        if (result.size() == 1) return result.getFirst();
        return result;
    }

    List<Map<String, Object>> selectRows() {
        return (List<Map<String, Object>>) select();
    }
    List<Article> selectRows(Class article) {
        return ((List<Map<String, Object>>) select()).stream().map(Article::convert).toList();
    }

    Map<String, Object> selectRow() {

        return (Map<String, Object>) select();
    }

    String getRowColumnName(Set a) {
        String name = "";
        for (Object i : a)
            name = (String)i;
        return name;
    }

    LocalDateTime selectDatetime() {
        return (LocalDateTime) selectRow().get(getRowColumnName(selectRow().keySet()));
    }

    long selectLong() {
        return (long) selectRow().get(getRowColumnName(selectRow().keySet()));
    }

    List<Long> selectLongs(){
        return selectRows().stream().mapToLong(e -> (long)e.get(getRowColumnName(e.keySet()))).boxed().collect(Collectors.toList());
    }

    String selectString() {
        return (String) selectRow().get(getRowColumnName(selectRow().keySet()));
    }

    boolean selectBoolean() {
        return (boolean) selectRow().get(getRowColumnName(selectRow().keySet()));

    }


}
