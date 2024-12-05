package com.ll.simpleDb;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;


public class Sql {
    StringBuilder sql = new StringBuilder();
    Connection con;
    boolean autoCommit = true;

    boolean isDev;

    Sql(Connection con,boolean autoCommit,boolean isDev){
        this.con = con;
        this.autoCommit = autoCommit;
        this.isDev = isDev;
    }
    void viewQeury(PreparedStatement stmt){
        if(isDev)
            System.out.println(stmt.toString());
    }
    void commit() throws SQLException {
        if(autoCommit)
            con.commit();
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
    Sql append(String query, Object... contents) {
        for (Object content : contents) {
            if(content instanceof String)
                query = query.replaceFirst("\\?", "'" + content + "'");
            else
                query = query.replaceFirst("\\?",   content.toString() );
        }
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
    Sql appendIn(String query, String... contents) {
        StringBuilder save = new StringBuilder();
        for(int i = 0; i< contents.length; i++) {
            save.append("'"+contents[i]+"'");
            if(i <contents.length-1) save.append(", ");
        }
        query = query.replaceFirst("\\?",save.toString());
        sql.append(query).append(" ");
        return this;
    }

    long runQeury() {
        long id = -1;
        try {
            Thread.sleep(50);
            PreparedStatement stmt = con.prepareStatement(sql.toString());
            viewQeury(stmt);
            id = stmt.executeUpdate();
            commit();
            stmt.close();
        } catch (SQLException | InterruptedException e) {
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
    long create(){
        return  runQeury();
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
    Object getColumnContent(ResultSet rs, String name, String type) throws SQLException {
        if(type.contains("BIGINT") && name.contains("COUNT"))
            return rs.getLong(name);
        if(type.contains("BIT") || type.contains("BIGINT"))
            return rs.getBoolean(name);
        if (type.contains("INT"))
            return rs.getLong(name);
        if (type.equals("DATETIME")) {
            return rs.getTimestamp(name).toLocalDateTime();
        }
        if (type.contains("VARCHAR") || type.equals("TEXT"))
            return rs.getString(name);

        return null;

    }

    Object select(boolean isSingle) {
        List<Map<String, Object>> result = new ArrayList<>();
        try {
            PreparedStatement stmt = con.prepareStatement(sql.toString());
            ResultSet rs = stmt.executeQuery();
            viewQeury(stmt);
            commit();
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
        if (isSingle) return result.getFirst();
        return result;
    }

    List<Map<String, Object>> selectRows() {
        return (List<Map<String, Object>>) select(false);
    }
    List<Article> selectRows(Class article) {
        return ((List<Map<String, Object>>) select(false)).stream().map(Article::convert).toList();
    }

    Article selectRow(Class article) {
        return Article.convert((Map<String, Object>) select(true));
    }
    Map<String, Object> selectRow() {
        return (Map<String, Object>) select(true);
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
