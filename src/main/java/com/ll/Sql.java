package com.ll;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;


public class Sql {
    private final Connection conn;
    private final StringBuilder sqlBuilder;
    private final List<Object> params;
    private boolean devMode;
    private ObjectMapper objectMapper;
    private final SimpleDb dataSource;


    public Sql(Connection conn, boolean devMode, SimpleDb dataSource) {
        this.sqlBuilder = new StringBuilder();
        this.conn = conn;
        this.params = new ArrayList<>();
        this.devMode = devMode;
        
        // Java Date / Time 지원 모듈 등록
        objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
        this.dataSource = dataSource;
    }

    /*
     * 쿼리 스트링을 스트링 빌더에 추가
     * 체이닝 가능하게 sql 타입 리턴
     *
     * @param  query 쿼리스트링
     * @return       sql 객체 자신
     * */
    public Sql append(String query) {
        sqlBuilder.append(" ").append(query);
        return this;
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
        sqlBuilder.append(" ").append(query);
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

        sqlBuilder.append(" ").append(query);
        this.params.addAll(Arrays.asList(params));
        return this;
    }

    /*
     * sql insert 수행 후 생성 된 id값 리턴
     *
     * @return   db로 부터 생성 된 id 값
     * */
    public long insert() {
        try {
            PreparedStatement ps = conn.prepareStatement(sqlBuilder.toString(), Statement.RETURN_GENERATED_KEYS);
            addParams(ps);
            loggingSql(ps);
            return ps.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            params.clear();
        }

        // 실패
        return -1L;
    }

    /*
     * SQL UPDATE 수행
     *
     * @return   1 : 성공, 0 : 실패
     * */
    public long update() {
        try {
            PreparedStatement ps = conn.prepareStatement(sqlBuilder.toString());
            addParams(ps);
            loggingSql(ps);
            int rs = ps.executeUpdate();
            ps.close();

            return rs;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        // 실패
        return -1L;
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
            loggingSql(ps);
            int rs = ps.executeUpdate();
            ps.close();

            return rs;
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
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
            e.printStackTrace();
        }
        // 데이터 없음
        return List.of();
    }

    public <T> List<T> selectRows(Class<T> tClass) {
        try {
            PreparedStatement ps = conn.prepareStatement(sqlBuilder.toString());
            loggingSql(ps);
            ResultSet rs = ps.executeQuery();
            List<T> results = new ArrayList<>();

            while (rs.next()) {
                ObjectNode node = objectMapper.createObjectNode();
                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();

                // 컬럼 데이터를 ObjectNode에 추가
                for (int i = 0; i < columnCount; i++) {
                    String columnName = metaData.getColumnLabel(i);
                    Object value = rs.getObject(i);
                    node.putPOJO(columnName, value);
                }

                // ObjectNode를 지정된 타입으로 매핑
                T row = objectMapper.convertValue(node, tClass);
                results.add(row);
//                long id = rs.getLong("id");
//                String title = rs.getString("title");
//                String body = rs.getString("body");
//                LocalDateTime createdDate = rs.getTimestamp("createdDate").toLocalDateTime();
//                LocalDateTime modifiedDate = rs.getTimestamp("modifiedDate").toLocalDateTime();
//                boolean isBlind = rs.getBoolean("isBlind");
//                results.add(new Article(
//                        id,
//                        title,
//                        body,
//                        createdDate,
//                        modifiedDate,
//                        isBlind
//                ));
            }

            return results;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        // null
        return List.of();
    }

    public Map<String, Object> selectRow() {
        try {
            PreparedStatement ps = conn.prepareStatement(sqlBuilder.toString());
            loggingSql(ps);
            ResultSet rs = ps.executeQuery();
            Map<String, Object> results = new HashMap<>();

            while (rs.next()) {
                results.put("id", rs.getLong("id"));
                results.put("title", rs.getString("title"));
                results.put("body", rs.getString("body"));
                results.put("createdDate", rs.getTimestamp("createdDate").toLocalDateTime());
                results.put("modifiedDate", rs.getTimestamp("modifiedDate").toLocalDateTime());
                results.put("isBlind", rs.getBoolean("isBlind"));
            }

            return results;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        // 데이터 없음
        return Map.of();
    }

    /*
     * 단 건의 ROW 조회 후, 클래스 타입으로 변환 후 반환
     *
     * @param  Class<T> 클래스 타입 정보
     * @return          클래스
     *
     * */
    public <T> T selectRow(Class<T> tClass) {
        String query = sqlBuilder.toString();
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(query);

                ResultSet rs = ps.executeQuery();
        ) {
            loggingSql(ps);
            if (rs.next()) {
                return (T) new Article(
                        rs.getLong("id"),
                        rs.getString("title"),
                        rs.getString("body"),
                        rs.getTimestamp("createdDate").toLocalDateTime(),
                        rs.getTimestamp("modifiedDate").toLocalDateTime(),
                        rs.getBoolean("isBlind")
                );
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        // 데이터 없음
        return null;
    }

    /*
    *
    *
    * */
    public LocalDateTime selectDatetime() {
        try(
            Statement stmt = conn.createStatement()
        ) {
            loggingSql(stmt);
            ResultSet rs = stmt.executeQuery(sqlBuilder.toString());
            LocalDateTime result = null;
            while (rs.next()) {
                 result = rs.getTimestamp("now()").toLocalDateTime();
            }
            rs.close();
            return result;
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // 실패
        return null;
    }

    /*
     * 단 건의 결과 행에서 특정 칼럼의 Long 값 반환
     *
     * @return   칼럼의 Long 값
     * */
    public Long selectLong() {
        try(
            PreparedStatement ps = conn.prepareStatement(sqlBuilder.toString())
        ) {
            addParams(ps);
            loggingSql(ps);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return rs.getLong(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
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
        try(
            PreparedStatement ps = conn.prepareStatement(sqlBuilder.toString());
            ResultSet rs = ps.executeQuery(sqlBuilder.toString());
        ){
            loggingSql(ps);
            if (rs.next()) {
                return rs.getString("title");
            }

        } catch (SQLException e) {
            e.printStackTrace();
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
        ){
            loggingSql(ps);
            boolean result = false;
            
            if (rs.next()) {
                result = rs.getBoolean(1);
            }
            rs.close();
            return result;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        // sql 실패시 기본 값인 false 반환
        return false;
    }

    /*
    * 질의 결과 값(Long)들을 리스트에 담아 반환
    *
    * @return List<Long> long 리스트 반환
    * */
    public List<Long> selectLongs() {
        try(
            PreparedStatement ps = conn.prepareStatement(sqlBuilder.toString());
        ){
            addParams(ps);
            loggingSql(ps);
            ResultSet rs = ps.executeQuery();

            List<Long> results = new ArrayList<>();

            while (rs.next()) {
                results.add(rs.getLong(1));
            }

            rs.close();
            return results;
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // null
        return List.of();
    }

    /*
    * 전달받은 PreparedStatement에 인자를 채우고 리스트를 초기화
    *
    * @param ps PreparedStatement
    * */
    private void addParams(PreparedStatement ps) throws SQLException {
        for (int i = 1; i <= params.size(); i++) {
            ps.setObject(i, params.get(i - 1));
        }
        params.clear();
    }

    /*
     * devMode true 로 설정 시 생성된 sql문 출력
     *
     * @param ps Statement 객체
     * */
    private void loggingSql(Statement ps) {
        if (devMode) {
            System.out.println(ps);
        }
    }
}
