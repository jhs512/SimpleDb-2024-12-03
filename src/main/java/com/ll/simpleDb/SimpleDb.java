package com.ll.simpleDb;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.RequiredArgsConstructor;

import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RequiredArgsConstructor
public class SimpleDb {
    private final String host;
    private final String username;
    private final String password;
    private final String dbName;
    private Connection connection;

    private static final ThreadLocal<Connection> threadLocalConnection = new ThreadLocal<>();

    // 데이터베이스 연결 초기화
    private Connection connect()  {
        Connection conn = threadLocalConnection.get();
        try {
            if (conn == null || conn.isClosed()) {
                // 새 Connection 생성 및 저장
                String url = String.format("jdbc:mysql://%s/%s?useSSL=false&allowPublicKeyRetrieval=true", host, dbName);
                conn = DriverManager.getConnection(url, username, password);
                threadLocalConnection.set(conn);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return conn;
    }

    public Sql genSql() {
        return new Sql(this);
    }

    public void closeConnection()  {

        Connection conn = threadLocalConnection.get();
        if (conn != null) {
            try {
                connection.close();
                conn.close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            threadLocalConnection.remove(); // 스레드에서 제거
        }
    }

    // SQL 실행 메서드
    public void run(String sql) {

        connection = connect(); // 연결 초기화
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to execute SQL: " + sql, e);
        }


    }


    public void run(String sql, String title, String body, boolean isBlind) {
        connection = connect();// 연결 초기화
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setString(1, title);
            pstmt.setString(2, body);
            pstmt.setBoolean(3, isBlind);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to execute SQL: " + sql, e);
        }

    }

    public void setDevMode(boolean b) {
    }



    public void startTransaction() {
        try {
            connection.setAutoCommit(false);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void rollback() {
        try {
            connection.rollback();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void commit() {
        try {
            connection.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public long insert(String sql, Object[] array){
        try (PreparedStatement pstmt = connection.prepareStatement(String.valueOf(sql), PreparedStatement.RETURN_GENERATED_KEYS)) {
            for (int i=0; i<array.length; i++){
                pstmt.setString(i+1, (String) array[i]);
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
            throw new RuntimeException(e);
        }
    }

    public int update(String sql, Object[] array) {
        try (PreparedStatement pstmt = connection.prepareStatement(String.valueOf(sql))) {
            for (int i=0; i<array.length; i++){
                pstmt.setString(i+1, (String) array[i]);
            }
            int row = pstmt.executeUpdate();
            return row;

        } catch (SQLException e) {
            throw new RuntimeException("Failed to execute SQL: " + sql, e);
        }
    }

    public int delete(String sql, Object[] array) {
        try (PreparedStatement pstmt = connection.prepareStatement(String.valueOf(sql))) {
            for (int i=0; i<array.length; i++){
                pstmt.setString(i+1, (String) array[i]);
            }
            int row = pstmt.executeUpdate();
            return row;

        } catch (SQLException e) {
            throw new RuntimeException("Failed to execute SQL: " + sql, e);
        }
    }

    public LocalDateTime selectDatetime(String sql, Object[] array) {
        try (PreparedStatement pstmt = connection.prepareStatement(String.valueOf(sql))) {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

            for (int i=0; i<array.length; i++){
                pstmt.setString(i+1, (String) array[i]);
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

    public List<Map<String, Object>> selectRows(String sql, Object[] array) {
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

    public <T> List<T> selectRows(String sql, Class<?> cls, Object[] array) {

        List<Map<String, Object>> list = selectRows(sql, array);
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());

        return list.stream()
                .map(obj -> (T) objectMapper.convertValue(obj, cls))
                .toList();
    }

    public Map<String, Object> selectRow(String sql, Object[] array) {
        try (PreparedStatement pstmt = connection.prepareStatement(String.valueOf(sql))) {
            Map<String, Object> map = new HashMap<>();
            for (int i=0; i<array.length; i++){
                pstmt.setString(i+1, (String) array[i]);
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

    public <T> T selectRow(String sql, Class<?> cls, Object[] array) {

        Map<String, Object> map = selectRow(sql, array);
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        Object Object = objectMapper.convertValue(map, cls);

        return (T) Object;
    }

    public Long selectLong(String sql, Object[] array) {
        try (PreparedStatement pstmt = connection.prepareStatement(String.valueOf(sql))) {

            for (int i=0; i<array.length; i++){
                pstmt.setString(i+1, (String) array[i]);
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

    public List<Long> selectLongs(String sql, Object[] array) {
        try (PreparedStatement pstmt = connection.prepareStatement(String.valueOf(sql))) {
            List<Long> longList = new ArrayList<>();

            for (int i=0; i<array.length; i++){
                pstmt.setString(i+1, (String) array[i]);
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

    public String selectString(String sql, Object[] array) {
        try (PreparedStatement pstmt = connection.prepareStatement(String.valueOf(sql))) {
            for (int i=0; i<array.length; i++){
                pstmt.setString(i+1, (String) array[i]);
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

    public Boolean selectBoolean(String sql, Object[] array) {
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