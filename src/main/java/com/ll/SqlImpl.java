package com.ll;

import java.lang.reflect.Field;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

public class SqlImpl implements Sql {

    public SqlImpl(Connection conn) {
        this.conn = conn;
    }

    private Connection conn;
    private PreparedStatement stmt;
    private StringBuilder query = new StringBuilder();
    private List<Object> params = new ArrayList<>();

    public SqlImpl append(String queryPiece, Object... values) {
        query.append(queryPiece);
        query.append(" ");

        if(queryPiece.replaceAll("[^?]", "").length() == values.length) {
            params.addAll(Arrays.asList(values));
        }else{
            // TODO : 파라미터 개수가 일치하지 않는다.
        }
        return this;
    }

    @Override
    public SqlImpl appendIn(String queryPiece, Object... values) {
        StringBuilder sb = new StringBuilder();
        for(Object value : values) {
            sb.append("'");
            sb.append(value);
            sb.append("',");
        }
        query.append(queryPiece.replaceFirst("\\?", sb.substring(0, sb.length()-1)));
        return this;
    }

    private void setQuery() throws SQLException {
        stmt = conn.prepareStatement(query.toString(), Statement.RETURN_GENERATED_KEYS);
        for (int i = 0; i < params.size(); i++) {
            stmt.setObject(i + 1, params.get(i));
        }
    }

    private long executeUpdate() {
        try{
            setQuery();
            return stmt.executeUpdate();
        }catch (SQLException e) {
            e.printStackTrace();
        }
        return 0;
    }

    private void executeQuery() throws SQLException {
        setQuery();
        stmt.executeQuery();
    }


    private final static long INVALID_ID = -1;
    @Override
    public long insert() {
        try{
            executeUpdate();
            ResultSet rs = stmt.getGeneratedKeys();
            if (rs.next()) {
                return rs.getLong(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // 키 생성이 안된 경우 or insert 가 아닌 다른 쿼리가 들어오는 경우
        return INVALID_ID;
    }

    @Override
    public long update() {
        return executeUpdate();
    }

    @Override
    public long delete() {
        return executeUpdate();
    }

    @Override
    public Map<String, Object> selectRow() {
        return selectRows().getFirst();
    }

    @Override
    public <T> T selectRow(Class<T> clazz) {
        return selectRows(clazz).getFirst();
    }

    @Override
    public LocalDateTime selectDatetime() {
        return LocalDateTime.now();
    }

    @Override
    public List<Map<String, Object>> selectRows() {
        List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
        try{
            executeQuery();
            ResultSet rs = stmt.getResultSet();
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                for(int i = 1; i <= columnCount; i++) {
                    row.put(metaData.getColumnName(i), rs.getObject(i));
                }
                list.add(row);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return list;
    }

    public <T> List<T> selectRows(Class<T> clazz) {
        List<T> list = new ArrayList<>();
        try{
            executeQuery();
            ResultSet rs = stmt.getResultSet();
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            while (rs.next()) {
                T obj = clazz.getDeclaredConstructor().newInstance(); // 기본 생성자 호출
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnName(i);
                    Object value = rs.getObject(columnName); // 컬럼 값 가져오기

                    // 자바 리플렉션을 사용해서 필드에 값을 설정
                    try {
                        Field field = clazz.getDeclaredField(columnName);
                        field.setAccessible(true);  // private 필드 접근 허용
                        field.set(obj, value);      // 필드에 값 설정
                    } catch (NoSuchFieldException e) {
                        // 해당 필드가 없을 경우 무시
                    }
                }
                list.add(obj);
            }
        }catch (Exception e) {
            e.printStackTrace();
        }
        return list;
    }

    private Object select(Object obj){
        Map<String, Object> row = selectRow();
        for(Object o : row.values()) {
            if(obj.getClass().isInstance(o.getClass())) {
                return o;
            }
        }
        return null;
    }

    private List<Object> selects(Object obj){
        List<Map<String, Object>> rows = selectRows();
        List<Object> list = new ArrayList<>();
        for(Map<String, Object> row : rows) {
            for(Object o : row.values()) {
                if(obj.getClass().isInstance(o.getClass())) {
                    list.add(o);
                }
            }
        }
        return list;
    }

    @Override
    public Long selectLong() {
        return (Long)select(Long.class);    // TODO : Optional 로 변환하기
    }

    @Override
    public String selectString() {
        return (String)select(String.class);
    }

    @Override
    public Boolean selectBoolean() {
        Object result = select(Boolean.class);
        return result != null && result == (Long)1L;
    }

    @Override
    public List<Long> selectLongs() {
        return selects(Long.class)
                .stream()
                .map((o-> (Long)o))
                .collect(Collectors.toList());
    }

    @Override
    public void close() throws SQLException {
        conn.close();
        conn = null;
    }
}
