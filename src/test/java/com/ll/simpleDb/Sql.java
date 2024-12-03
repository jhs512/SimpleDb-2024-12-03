package com.ll.simpleDb;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.IntStream;

public class Sql {

    // 입력받은 sql 저장할 변수
    String sql = "";

    // sql 실행하기 위한 Statement 객체
    Statement stat;

    /**
     *
     * Sql 생성
     *
     * @param stat 주입받을 Satatement 객체
     *
     * @author shjung
     * @since 2024. 12. 02.
     */
    public Sql(Statement stat){
        this.stat = stat;
    }

    /**
     *
     * sql 입력받거나 추가하기 위한 함수
     * - ? -> 하나의 입력값 삽입
     *
     * @param str sql 문자열
     * @param objects 입력값인 Object 배열
     * @return Sql 자기 자신
     *
     * @author shjung
     * @since 2024. 12. 02.
     */
    public Sql append(String str, Object... objects){
        sql += str + " ";
        if(objects.length > 0){
            for (int i = 0; i < objects.length; i++){
                sql = sql.replaceFirst("\\?", objects[i] instanceof String ? "'" + objects[i] + "'" : objects[i].toString()) + " ";
            }
        }

        return this;
    }

    /**
     *
     * sql 입력받거나 추가하기 위한 함수
     * - 여러 개의 입력값을 하나의 ?에 넣는 함수
     * - 입력값을 객체별로 받는 경우
     *
     * @param objects 첫번째로 받는 sql값과 그 입력값들
     * @return Sql 자기 자신
     *
     * @author shjung
     * @since 2024. 12. 02.
     */
    public Sql appendIn(Object... objects){
        sql += objects[0].toString() + " ";

        if(objects.length > 1){
            String s;
            if(objects[1] instanceof String){
                s = "'" + objects[1] + "'";
            }
            else {
                s = objects[1].toString();
            }
            for(int i = 2; i < objects.length; i++){
                if(objects[i] instanceof String){
                    s += ", '" + objects[i].toString() + "' ";
                }else {
                    s += ", " + objects[i].toString() + " ";
                }

            }
            sql = sql.replace("?", s);
        }

        return this;
    }

    /**
     *
     * sql 입력받거나 추가하기 위한 함수
     * - 여러 개의 입력값을 하나의 ?에 넣는 함수
     * - 입력값을 객체별로 오는 것이 아닌 배열로 받는 경우
     *
     * @param str sql 문자열
     * @param objects 문자열에 추가될 Object 배열
     * @return Sql 자기 자신
     *
     * @author shjung
     * @since 2024. 12. 02.
     */
    public Sql appendIn(String str, Object[] objects){
        this.sql += str + " ";

        if(objects.length > 0){
            String s = objects[0] instanceof String ? "'" + objects[0] + "' " : objects[0].toString();
            for(int i = 1; i < objects.length; i++){
                if(objects[i] instanceof String){
                    s += ", '" + objects[i] + "'";
                }
                else{
                    s += ", " + objects[i].toString();
                }

            }
            this.sql = sql.replace("?", s);
        }
        return this;
    }

    /**
     *
     * 입력한 값을 DB에 삽입하는 쿼리를 실행하는 함수
     *
     * @return 삽입된 개수 리턴
     *
     * @author shjung
     * @since 2024. 12. 02.
     */
    public long insert(){
        try {
            return (long) this.stat.executeUpdate(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     *
     * DB에 입력된 값을 수정 쿼리를 실행하는 함수
     *
     * @return 수정된 개수
     *
     * @author shjung
     * @since 2024. 12. 02.
     */
    public long update(){
        try {
            return (long) this.stat.executeUpdate(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     *
     * DB에서 특정한 값을 삭제하는 쿼리를 실행하는 함수
     *
     * @return 삭제된 개수
     *
     * @author shjung
     * @since 2024. 12. 02.
     */
    public long delete(){
        try {
            return (long) this.stat.executeUpdate(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     *
     * 목록 조회하는 함수
     *
     * @return Map<String, Object> 형식으로 만들어진 결과 리스트
     *
     * @author shjung
     * @since 2024. 12. 02.
     */
    public List<Map<String, Object>> selectRows(){
        try {
            List<Map<String, Object>> list = new ArrayList<>();

            ResultSet rs = this.stat.executeQuery(sql);

            ResultSetMetaData rsmd = rs.getMetaData();

            int numCols = rsmd.getColumnCount();

            List<String> colNames = IntStream.rangeClosed(1, numCols)
                    .mapToObj(i -> {
                        try {
                            return rsmd.getColumnName(i);
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                    }).toList();

            while(rs.next()){
                Map<String, Object> map = new HashMap<>();

                colNames.forEach(colName -> {
                    try {
                        map.put(colName, rs.getObject(colName));
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                });

                list.add(map);
            }

            return list;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     *
     * 클래스 타입으로 구분되는 목록 조회
     *
     * @param ignoredTClass 클래스 타입
     * @return 제네릭 타입 목록
     * @param <T> 제네릭 타입
     *
     * @author shjung
     * @since 2024. 12. 02.
     */
    public <T> List<T> selectRows(Class<T> ignoredTClass){
        try {
            List<T> list = new ArrayList<>();

            ObjectMapper objectMapper = new ObjectMapper();
            // Local Date Time 설정
            objectMapper.registerModule(new JavaTimeModule());
            // boolean 값을 필드의 이름에 맞춰서 변경하기 위한 설정
            objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
            ResultSet rs = this.stat.executeQuery(sql);

            ResultSetMetaData rsmd = rs.getMetaData();

            int numCols = rsmd.getColumnCount();

            List<String> colNames = IntStream.rangeClosed(1, numCols)
                    .mapToObj(i -> {
                        try {
                            return rsmd.getColumnName(i);
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                    }).toList();

            while(rs.next()){
                Map<String, Object> map = new HashMap<>();

                colNames.forEach(colName -> {
                    try {
                        map.put(colName, rs.getObject(colName));
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                });

                T t = objectMapper.convertValue(map, ignoredTClass);
                list.add(t);
            }

            return list;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     *
     * sql로 객체 하나 조회
     *
     * @return Map<String, Object> 객체
     */
    public Map<String, Object> selectRow(){
        try {
            ResultSet rs = this.stat.executeQuery(sql);

            ResultSetMetaData rsmd = rs.getMetaData();

            int numCols = rsmd.getColumnCount();

            List<String> colNames = IntStream.rangeClosed(1, numCols)
                    .mapToObj(i -> {
                        try {
                            return rsmd.getColumnName(i);
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                    }).toList();

            Map<String, Object> map = new HashMap<>();
            while(rs.next()){
                colNames.forEach(colName -> {
                    try {
                        map.put(colName, rs.getObject(colName));
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                });
            }

            return map;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     *
     * 객체 하나 조회 함수
     *
     * @param ignoredTClass 클래스 타입
     * @return 객체
     * @param <T> 제네릭 타입
     *
     * @author shjung
     * @since 2024. 12. 02.
     */
    public <T> T selectRow(Class<T> ignoredTClass){
        ObjectMapper objectMapper = new ObjectMapper();
        // Local Date Time 설정
        objectMapper.registerModule(new JavaTimeModule());
        // boolean 값을 필드의 이름에 맞춰서 변경하기 위한 설정
        objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
        try {
            ResultSet rs = this.stat.executeQuery(sql);

            ResultSetMetaData rsmd = rs.getMetaData();

            int numCols = rsmd.getColumnCount();

            List<String> colNames = IntStream.rangeClosed(1, numCols)
                    .mapToObj(i -> {
                        try {
                            return rsmd.getColumnName(i);
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                    }).toList();

            Map<String, Object> map = new HashMap<>();
            while(rs.next()){
                colNames.forEach(colName -> {
                    try {
                        map.put(colName, rs.getObject(colName));
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                });
            }

            return objectMapper.convertValue(map, ignoredTClass);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     *
     * 시간 조회 함수
     *
     * @return 시간
     *
     * @author shjung
     * @since 2024. 12. 02.
     */
    public LocalDateTime selectDatetime(){
        try {
            ResultSet rs = this.stat.executeQuery(sql);
            return rs.next() ? rs.getTimestamp(1).toLocalDateTime() : null;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     *
     * Long 타입 조회 함수
     *
     * @return Long
     *
     * @author shjung
     * @since 2024. 12. 02.
     */
    public Long selectLong(){
        try {
            ResultSet rs = this.stat.executeQuery(sql);
            return rs.next() ? rs.getLong(1) : null;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     *
     * 문자열 조회 함수
     *
     * @return 문자열
     *
     * @author shjung
     * @since 2024. 12. 02.
     */
    public String selectString(){
        try {
            ResultSet rs = this.stat.executeQuery(sql);
            return rs.next() ? rs.getString(1) : null;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     *
     * boolean 값 조회 함수
     *
     * @return boolean 값
     *
     * @author shjung
     * @since 2024. 12. 02.
     */
    public Boolean selectBoolean(){
        try {
            ResultSet rs = this.stat.executeQuery(sql);
            return rs.next() ? rs.getBoolean(1) : null;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     *
     * Long List 값 조회 함수
     *
     * @return List<Long> 객체
     *
     * @author shjung
     * @since 2024. 12. 02.
     */
    public List<Long> selectLongs(){
        try {
            ResultSet rs = this.stat.executeQuery(sql);

            List<Long> list = new ArrayList<>();

            while(rs.next()){
                long l = rs.getLong(1);
                list.add(l);
            }

            return list;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}