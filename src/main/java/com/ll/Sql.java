package com.ll;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;


public class Sql {
    private final Connection conn;
    private StringBuilder sqlBuilder;
    private List<Object> params;
    private boolean devMode;

    public Sql(Connection conn, boolean devMode) {
        this.sqlBuilder = new StringBuilder();
        this.conn = conn;
        this.params = new ArrayList<>();
        this.devMode = devMode;
    }

    // 단순 쿼리 더하기
    public Sql append(String query) {
        sqlBuilder.append(" ").append(query);
        return this;
    }
    
    // 가변 인자 활용
    public Sql append(String query, Object ... params ) {
        sqlBuilder.append(" ").append(query);
        this.params.addAll(Arrays.asList(params));
        return this;
    }

    public long insert() {
        try {
            PreparedStatement ps = conn.prepareStatement(sqlBuilder.toString(), Statement.RETURN_GENERATED_KEYS);

            for(int i=1;i<=params.size();i++){
                ps.setObject(i, params.get(i-1));
            }
            loggingSql(ps);
            int id = ps.executeUpdate();
            ps.close();

            return id;
        }catch(SQLException e){
            e.printStackTrace();
        }finally {
            params.clear();
        }
        
        // 실패
        return -1;
    }

    public long update() {
        try {
            PreparedStatement ps = conn.prepareStatement(sqlBuilder.toString());

            for(int i=1;i<=params.size();i++){
                ps.setObject(i, params.get(i-1));
            }
            loggingSql(ps);
            int rs = ps.executeUpdate();
            ps.close();

            return rs;
        }catch(SQLException e){
            e.printStackTrace();
        }finally {
            params.clear();
        }
        // 실패
        return -1;
    }

    public long delete() {
        try {
            PreparedStatement ps = conn.prepareStatement(sqlBuilder.toString());

            for(int i=1;i<=params.size();i++){
                ps.setObject(i, params.get(i-1));
            }
            loggingSql(ps);
            int rs = ps.executeUpdate();
            ps.close();

            return rs;
        }catch(SQLException e){
            e.printStackTrace();
        }finally {
            params.clear();
        }

        return -1;
    }

    public List<Map<String, Object>> selectRows() {
        try {
            PreparedStatement ps = conn.prepareStatement(sqlBuilder.toString());
            loggingSql(ps);
            ResultSet rs = ps.executeQuery();

            List<Map<String, Object>> results = new ArrayList<>();

            while(rs.next()){
                Map<String, Object> row = new HashMap<>();
                row.put("id", rs.getLong("id"));
                row.put("title", rs.getString("title"));
                row.put("body", rs.getString("body"));


                row.put("createdDate", rs.getTimestamp("createdDate").toLocalDateTime());
                row.put("modifiedDate", rs.getTimestamp("modifiedDate").toLocalDateTime());
                row.put("isBlind", rs.getBoolean("isBlind"));
                results.add(row);
            }

            return results;
        }catch(SQLException e){
            e.printStackTrace();
        }
        // 데이터 없음
        return List.of();
    }

    public Map<String, Object> selectRow() {
        try {
            PreparedStatement ps = conn.prepareStatement(sqlBuilder.toString());
            loggingSql(ps);
            ResultSet rs = ps.executeQuery();
            Map<String, Object> results = new HashMap<>();

            while(rs.next()){
                results.put("id", rs.getLong("id"));
                results.put("title", rs.getString("title"));
                results.put("body", rs.getString("body"));
                results.put("createdDate", rs.getTimestamp("createdDate").toLocalDateTime());
                results.put("modifiedDate", rs.getTimestamp("modifiedDate").toLocalDateTime());
                results.put("isBlind", rs.getBoolean("isBlind"));
            }

            return results;
        }catch(SQLException e){
            e.printStackTrace();
        }
        // 데이터 없음
        return Map.of();
    }

    public LocalDateTime selectDatetime() {
        try {
            Statement stmt = conn.createStatement();
            loggingSql(stmt);
            ResultSet rs = stmt.executeQuery(sqlBuilder.toString());
            while(rs.next()){
                return rs.getTimestamp("now()").toLocalDateTime();
            }

        }catch(SQLException e){
            e.printStackTrace();
        }

        // 실패
        return null;
    }

    public Long selectLong() {
        try {
            PreparedStatement ps = conn.prepareStatement(sqlBuilder.toString());

            // 파라미터 있으면 추가
            for(int i=1;i<=params.size();i++){
                ps.setObject(i, params.get(i-1));
            }

            loggingSql(ps);
            ResultSet rs = ps.executeQuery();

            while(rs.next()){
                return rs.getLong(1);
            }

        }catch(SQLException e){
            e.printStackTrace();
        }

        // 실패
        return 0L;
    }

    public String selectString() {
        try {
            PreparedStatement ps = conn.prepareStatement(sqlBuilder.toString());
            loggingSql(ps);

            ResultSet rs = ps.executeQuery(sqlBuilder.toString());
            while(rs.next()){
                return rs.getString("title");
            }

        }catch(SQLException e){
            e.printStackTrace();
        }

        // 실패
        return null;
    }

    public Boolean selectBoolean() {
        try {
            PreparedStatement ps = conn.prepareStatement(sqlBuilder.toString());
            loggingSql(ps);

            ResultSet rs = ps.executeQuery(sqlBuilder.toString());
            while(rs.next()){
                return rs.getBoolean(1);
            }

        }catch(SQLException e){
            e.printStackTrace();
        }

        // 실패
        return false;
    }

    private void loggingSql(Statement ps){
        if(devMode){
            System.out.println(ps);
        }
    }
}
