package com.ll.simpleDb;

import com.ll.Article;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class Sql {
    private Connection connection;
    private StringBuilder sql;
    private List<Object> param;

    public Sql(Connection connection){
        this.connection = connection;
        this.sql = new StringBuilder();
        this.param = new ArrayList<>();
    }

    public Sql append(Object... params) {
        for(int i=0; i<params.length; i++){
            if(i>0){
                param.add(String.valueOf(params[i]));
            }else{
                sql.append(params[i]).append(" ");
            }
        }
        return this;
    }

    public Sql appendIn(String s, Object... params) {

        String placeholders = String.join(",", Collections.nCopies(params.length, "?"));
        sql.append(s.replace("?", placeholders));

        for (Object o : params) {
            param.add(String.valueOf(o));
        }

        return this;
    }

    public long insert() {
        try (PreparedStatement pstmt = connection.prepareStatement(String.valueOf(sql), PreparedStatement.RETURN_GENERATED_KEYS)) {
            for (int i=0; i<param.size(); i++){
                pstmt.setString(i+1, (String) param.get(i));
            }
            pstmt.executeUpdate();

            try (ResultSet generatedKeys = pstmt.getGeneratedKeys()) {
                if (generatedKeys.next()) {
                    return generatedKeys.getLong(1); // 첫 번째 열의 값을 가져옴
                } else {
                    System.out.println("생성된 ID를 찾을 수 없습니다.");
                    return 0;
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to execute SQL: " + sql, e);
        }
    }

    public int update() {
        try (PreparedStatement pstmt = connection.prepareStatement(String.valueOf(sql))) {
            for (int i=0; i<param.size(); i++){
                pstmt.setString(i+1, (String) param.get(i));
            }
            int row = pstmt.executeUpdate();
            return row;

        } catch (SQLException e) {
            throw new RuntimeException("Failed to execute SQL: " + sql, e);
        }
    }

    public int delete() {
        try (PreparedStatement pstmt = connection.prepareStatement(String.valueOf(sql))) {
            for (int i=0; i<param.size(); i++){
                pstmt.setString(i+1, (String) param.get(i));
            }
            int row = pstmt.executeUpdate();
            return row;

        } catch (SQLException e) {
            throw new RuntimeException("Failed to execute SQL: " + sql, e);
        }
    }

    public List<Map<String, Object>> selectRows() {
        try (PreparedStatement pstmt = connection.prepareStatement(String.valueOf(sql))) {
            List<Map<String, Object>> list = new ArrayList<>();

            ResultSet rs = pstmt.executeQuery(); // 쿼리 실행
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

            while (rs.next()) {
                Map<String, Object> map = new HashMap<>();
                map.put("id", rs.getLong("id"));
                map.put("createdDate", LocalDateTime.parse(rs.getString("createdDate"), formatter));
                map.put("modifiedDate", LocalDateTime.parse(rs.getString("modifiedDate"), formatter));
                map.put("title", rs.getString("title"));
                map.put("body", rs.getString("body"));
                map.put("isBlind", rs.getBoolean("isBlind"));
                list.add(map);
            }
            return list;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to execute SQL: " + sql, e);
        }
    }

    public List<Article> selectRows(Class<Article> articleClass) {
        try (PreparedStatement pstmt = connection.prepareStatement(String.valueOf(sql))) {
            List<Article> list = new ArrayList<>();

            ResultSet rs = pstmt.executeQuery(); // 쿼리 실행
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

            while (rs.next()) {
                list.add(new Article(
                        rs.getLong("id"),
                        LocalDateTime.parse(rs.getString("createdDate"), formatter),
                        LocalDateTime.parse(rs.getString("modifiedDate"), formatter),
                        rs.getString("title"),
                        rs.getString("body"),
                        rs.getBoolean("isBlind")
                        ));
            }
            return list;

        } catch (SQLException e) {
            throw new RuntimeException("Failed to execute SQL: " + sql, e);
        }
    }

    public Map<String, Object> selectRow() {

        try (PreparedStatement pstmt = connection.prepareStatement(String.valueOf(sql))) {
            Map<String, Object> map = new HashMap<>();
            for (int i=0; i<param.size(); i++){
                pstmt.setString(i+1, (String) param.get(i));
            }

            ResultSet rs = pstmt.executeQuery(); // 쿼리 실행
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

            while (rs.next()) {
                map.put("id", rs.getLong("id"));
                map.put("createdDate", LocalDateTime.parse(rs.getString("createdDate"), formatter));
                map.put("modifiedDate", LocalDateTime.parse(rs.getString("modifiedDate"), formatter));
                map.put("title", rs.getString("title"));
                map.put("body", rs.getString("body"));
                map.put("isBlind", rs.getBoolean("isBlind"));
            }
            return map;

        } catch (SQLException e) {
            throw new RuntimeException("Failed to execute SQL: " + sql, e);
        }
    }

    public Article selectRow(Class<Article> articleClass) {

        try (PreparedStatement pstmt = connection.prepareStatement(String.valueOf(sql))) {

            for (int i=0; i<param.size(); i++){
                pstmt.setString(i+1, (String) param.get(i));
            }

            ResultSet rs = pstmt.executeQuery(); // 쿼리 실행
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

            while (rs.next()) {
                return new Article(
                        rs.getLong("id"),
                        LocalDateTime.parse(rs.getString("createdDate"), formatter),
                        LocalDateTime.parse(rs.getString("modifiedDate"), formatter),
                        rs.getString("title"),
                        rs.getString("body"),
                        rs.getBoolean("isBlind")
                );
            }

            return null;

        } catch (SQLException e) {
            throw new RuntimeException("Failed to execute SQL: " + sql, e);
        }
    }

    public LocalDateTime selectDatetime() {
        try (PreparedStatement pstmt = connection.prepareStatement(String.valueOf(sql))) {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

            for (int i=0; i<param.size(); i++){
                pstmt.setString(i+1, (String) param.get(i));
            }
            ResultSet rs = pstmt.executeQuery();

            while (rs.next()) {
                return LocalDateTime.parse(rs.getString(1), formatter);
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to execute SQL: " + sql, e);
        }
        return null;
    }

    public Long selectLong() {
        try (PreparedStatement pstmt = connection.prepareStatement(String.valueOf(sql))) {

            for (int i=0; i<param.size(); i++){
                pstmt.setString(i+1, (String) param.get(i));
            }

            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                return rs.getLong(1);
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to execute SQL: " + sql, e);
        }
        return null;
    }

    public List<Long> selectLongs() {
        try (PreparedStatement pstmt = connection.prepareStatement(String.valueOf(sql))) {
            List<Long> longList = new ArrayList<>();

            for (int i=0; i<param.size(); i++){
                pstmt.setString(i+1, (String) param.get(i));
            }

            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                longList.add(rs.getLong(1));
            }

            return longList;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to execute SQL: " + sql, e);
        }
    }

    public String selectString() {
        try (PreparedStatement pstmt = connection.prepareStatement(String.valueOf(sql))) {
            for (int i=0; i<param.size(); i++){
                pstmt.setString(i+1, (String) param.get(i));
            }

            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                return rs.getString("title");
            }

        } catch (SQLException e) {
            throw new RuntimeException("Failed to execute SQL: " + sql, e);
        }
        return null;
    }

    public Boolean selectBoolean() {
        try (PreparedStatement pstmt = connection.prepareStatement(String.valueOf(sql))) {
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                return rs.getBoolean(1);
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to execute SQL: " + sql, e);
        }
        return null;
    }



}
