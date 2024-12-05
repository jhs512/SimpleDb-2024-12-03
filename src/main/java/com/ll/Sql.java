package com.ll;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import javax.sql.DataSource;
import javax.xml.crypto.Data;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;


public class Sql {
    private final Connection conn;
    private final StringBuilder sqlBuilder;
    private final List<Object> params;
    private final ObjectMapper objectMapper;
    private final SimpleDb simpleDb;

    public Sql(SimpleDb simpleDb) {
        this.sqlBuilder = new StringBuilder();
        this.simpleDb = simpleDb;
        this.conn = simpleDb.getConnection();

        this.params = new ArrayList<>();
        // Java Date / Time 지원 모듈 등록
        this.objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    }

    /*
     * 쿼리 스트링을 스트링 빌더에 추가
     * 가변인자로 받은 파라미터를 배열에 추가
     * 체이닝 가능하게 sql 타입 리턴
     *
     * @param query    쿼리 스트링
     * @param params 가변인자 배열
     * @return       sql 객체 자신
     * */
    public Sql append(String query, Object... params) {
        sqlBuilder.append("\n").append(query);
        this.params.addAll(Arrays.asList(params));
        return this;
    }

    /*
     * 쿼리 스트링을 스트링 빌더에 추가
     * 쿼리 스트링 내 '?' 개수를 파라미터 개수에 맞게 재구성
     * 가변인자로 받은 파라미터를 배열에 추가
     * 체이닝 가능하게 sql 타입 리턴
     *
     * @param query    쿼리 스트링
     * @param params 가변인자 배열
     * @return       sql 객체 자신
     * */
    public Sql appendIn(String query, Object... params) {
        String placeHolders = String.join(", ", Collections.nCopies(params.length, "?"));
        query = query.replace("?", placeHolders);
        return append(query, params);
    }

    /*
     * sql insert 수행 후 생성 된 id값 리턴
     *
     * @return   db로 부터 생성 된 id 값
     * */
    public long insert() {
        return simpleDb.insert(sqlBuilder.toString(), params.toArray());
    }

    /*
     * SQL UPDATE 수행
     *
     * @return   1 : 성공, 0 : 실패
     * */
    public long update() {
        return simpleDb.update(sqlBuilder.toString(), params.toArray());
    }

    /*
     * SQL DELETE 함수
     *
     * @return   affected Rows
     * */
    public long delete() {
        try {
            PreparedStatement ps = conn.prepareStatement(sqlBuilder.toString());

            for (int i = 1; i <= params.size(); i++) {
                ps.setObject(i, params.get(i - 1));
            }
            int rs = ps.executeUpdate();
            ps.close();
            return rs;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            params.clear();
        }
    }

    public List<Map<String, Object>> selectRows() {
        try {
            PreparedStatement ps = conn.prepareStatement(sqlBuilder.toString());
            ResultSet rs = ps.executeQuery();

            List<Map<String, Object>> results = new ArrayList<>();
            while (rs.next()) {
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
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /*
     * SELECT 수행 후 결과 값을 클래스에 매핑해 리스트로 반환
     *
     * @param  class<T>  클래스 타입
     * @return           List<T>
     * */
    public <T> List<T> selectRows(Class<T> tClass) {
        try {
            PreparedStatement ps = conn.prepareStatement(sqlBuilder.toString());
            ResultSet rs = ps.executeQuery();
            List<T> results = new ArrayList<>();

            while (rs.next()) {
                ObjectNode node = objectMapper.createObjectNode();
                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();

                // 컬럼 데이터를 ObjectNode에 추가
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnLabel(i);
                    Object value = rs.getObject(i);
                    node.putPOJO(columnName, value);
                }

                // ObjectNode를 지정된 타입으로 매핑
                T row = objectMapper.convertValue(node, tClass);
                results.add(row);
            }

            return results;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /*
     * 단 건의 SELECT 결과를 Map으로 변환 해 반환
     *
     * @return   Map<String, Object>
     * */
    public Map<String, Object> selectRow() {
        try {
            PreparedStatement ps = conn.prepareStatement(sqlBuilder.toString());
            ResultSet rs = ps.executeQuery();
            Map<String, Object> result = new HashMap<>();

            if (rs.next()) {
                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();

                // 컬럼 데이터를 Map 추가
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnLabel(i);
                    Object value = rs.getObject(i);
                    result.put(columnName, value);
                }
            }

            return result;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /*
     * 단 건의 ROW 조회 후, 클래스 타입으로 변환 후 반환
     * 멀티스레딩 오류 있음
     * @param  Class<T> 클래스 타입 정보
     * @return          클래스
     *
     * */
    public <T> T selectRow(Class<T> tClass) {
        String query = sqlBuilder.toString();
        try (
                PreparedStatement ps = conn.prepareStatement(query);
                ResultSet rs = ps.executeQuery();
        ){
            if (rs.next()) {
                ObjectNode node = objectMapper.createObjectNode();
                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();

                // 컬럼 데이터를 ObjectNode에 추가
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnLabel(i);
                    Object value = rs.getObject(i);
                    node.putPOJO(columnName, value);
                }

                // ObjectNode를 지정된 타입으로 매핑
                return objectMapper.convertValue(node, tClass);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        // 데이터 없음
        return null;
    }

    /*
     * 칼럼의 시간 값을 읽어 LocalDateTime 으로 변환 후 반환
     *
     * @return   LocalDateTime
     * */
    public LocalDateTime selectDatetime() {
        try (
            Statement stmt = conn.createStatement()
        ) {
            ResultSet rs = stmt.executeQuery(sqlBuilder.toString());
            LocalDateTime result = null;
            while (rs.next()) {
                result = rs.getTimestamp("now()").toLocalDateTime();
            }
            rs.close();
            return result;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /*
     * 단 건의 결과 행에서 특정 칼럼의 Long 값 반환
     *
     * @return   칼럼의 Long 값
     * */
    public Long selectLong() {
        try (
            PreparedStatement ps = conn.prepareStatement(sqlBuilder.toString())
        ) {

            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return rs.getLong(1);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        // 실패
        return 0L;
    }


    /*
     * 단 건의 결과 행에서 특정 칼럼의 String 값 반환
     *
     * @return   칼럼의 String 값
     * */
    public String selectString() {
        try (
                PreparedStatement ps = conn.prepareStatement(sqlBuilder.toString());
                ResultSet rs = ps.executeQuery(sqlBuilder.toString());
        ) {
            if (rs.next()) {
                return rs.getString("title");
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        // 실패
        return null;
    }

    /*
     * 단 건의 결과 행에서 boolean 값을 반환
     *
     * @return   칼럼의 boolean 값 (실패 시 false)
     * */
    public Boolean selectBoolean() {
        try (
                PreparedStatement ps = conn.prepareStatement(sqlBuilder.toString());
                ResultSet rs = ps.executeQuery(sqlBuilder.toString());
        ) {
            boolean result = false;

            if (rs.next()) {
                result = rs.getBoolean(1);
            }
            rs.close();
            return result;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /*
     * 질의 결과 값(Long)들을 리스트에 담아 반환
     *
     * @return List<Long> long 리스트 반환
     * */
    public List<Long> selectLongs() {
        try (
                PreparedStatement ps = conn.prepareStatement(sqlBuilder.toString());
        ) {
            ResultSet rs = ps.executeQuery();

            List<Long> results = new ArrayList<>();

            while (rs.next()) {
                results.add(rs.getLong(1));
            }

            rs.close();
            return results;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }




}
