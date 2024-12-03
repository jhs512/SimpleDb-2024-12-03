package org.example;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;

public class Sql {
    private Connection connection;
    private StringBuilder sb;
    private List<Object> params;

    private boolean devMode = false;

    public Sql(Connection connection, boolean devMode) {
        this.connection = connection;
        this.sb = new StringBuilder();
        this.params = new ArrayList<>();
        this.devMode = devMode;
    }

    public Sql append(String sql, Object... args) {
        sb.append(sql).append(" ");
        params.addAll(Arrays.asList(args));
        return this;
    }

    public Sql appendIn(String sql, Object... args){
        sb.append(sql.replace("?", replaceQuestionMark(args))).append(" ");
        params.clear(); // 이미 parameter 바인딩 했으므로 list 초기화
        return this;
    }

    private String replaceQuestionMark(Object[] args) {
        StringBuilder sb = new StringBuilder();
        for(int i=0; i< args.length; i++){
            sb.append("'").append(args[i].toString()).append("'");
            if(i < args.length - 1){
                sb.append(", ");
            }
        }
        return sb.toString();
    }

    private String reformat(Object param) {
        if (param instanceof String) {
            return "'" + param.toString().replace("'", "''") + "'";
        }
        return param.toString();
    }

    private String replaceWithParams(String sql, Object... params) {
        String[] strings = sql.split("\\?");
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < params.length; i++) {
            sb.append(strings[i]);
            sb.append(reformat(params[i]));
        }

        if(strings.length > params.length) {
            sb.append(strings[params.length]);
        }

        String[] lines = sb.toString().split(",");
        return Arrays.stream(lines)
                .map(String::trim)
                .reduce((line1, line2) -> line1 + " ,\n" + line2)
                .orElse(""); // 빈 쿼리일 경우 처리
    }

    private void showQuery(){
        String sql = sb.toString().trim();

        if (devMode) {
            // 개발 모드일 때 쿼리와 파라미터 출력
            System.out.println("== rawSql ==");
            String formattedSql = replaceWithParams(sql, params.toArray());
            System.out.println(formattedSql);
        }
    }

    private void setParam(PreparedStatement preparedStatement) throws SQLException {
        for (int i = 0; i < params.size(); i++) {
            preparedStatement.setObject(i + 1, params.get(i));
        }
    }

    public long insert() {
        String sql = sb.toString().trim();

        showQuery();

        try {
            PreparedStatement preparedStatement = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);

            setParam(preparedStatement);

            int row = preparedStatement.executeUpdate();

            if (row == 0) {
                throw new SQLException("생성을 수행하지않았습니다.");
            }

            try (ResultSet rs = preparedStatement.getGeneratedKeys()) {
                if (rs.next()) {
                    return rs.getLong(1);
                }
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        throw new RuntimeException("생성에 실패하였습니다.");
    }

    public long update() {
        String sql = sb.toString().trim();

        showQuery();

        try {
            PreparedStatement preparedStatement = connection.prepareStatement(sql);

            setParam(preparedStatement);

            return preparedStatement.executeUpdate();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


    public long delete() {
        String sql = sb.toString().trim();

        showQuery();

        try {
            PreparedStatement preparedStatement = connection.prepareStatement(sql);

            setParam(preparedStatement);

            return preparedStatement.executeUpdate();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public List<Map<String, Object>> selectRows() {
        String sql = sb.toString().trim();

        showQuery();

        try {
            PreparedStatement preparedStatement = connection.prepareStatement(sql);

            ResultSet rs = preparedStatement.executeQuery();
            List<Map<String, Object>> rows = new ArrayList<>();
            ResultSetMetaData metaData = rs.getMetaData();
            int count = metaData.getColumnCount();

            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                for (int i = 1; i <= count; i++) {
                    String columnName = metaData.getColumnName(i);
                    Object value = rs.getObject(i);

                    if (value instanceof Timestamp) {
                        value = ((Timestamp) value).toLocalDateTime();
                    }

                    row.put(columnName, value);
                }
                rows.add(row);
            }
            return rows;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public <T> List<T> selectRows(Class<T> clazz) {
        String sql = sb.toString().trim();

        showQuery();

        try {
            PreparedStatement preparedStatement = connection.prepareStatement(sql);

            ResultSet rs = preparedStatement.executeQuery();
            ResultSetMetaData metaData = rs.getMetaData();
            int count = metaData.getColumnCount();

            List<T> result = new ArrayList<>();

            while (rs.next()) {
                T t = clazz.getDeclaredConstructor().newInstance();

                for (int i = 1; i <= count; i++) {
                    String columnName = metaData.getColumnName(i);
                    Object value = rs.getObject(i);

                    if (value instanceof Timestamp) {
                        value = ((Timestamp) value).toLocalDateTime();
                    }

                    Field field = clazz.getDeclaredField(columnName);
                    field.setAccessible(true);
                    field.set(t, value);
                }
                result.add(t);
            }
            return result;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    public Map<String, Object> selectRow() {
        String sql = sb.toString().trim();

        showQuery();

        try {
            PreparedStatement preparedStatement = connection.prepareStatement(sql);

            ResultSet rs = preparedStatement.executeQuery();
            if (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                ResultSetMetaData metaData = rs.getMetaData();
                int count = metaData.getColumnCount();

                for (int i = 1; i <= count; i++) {
                    String columName = metaData.getColumnName(i);
                    Object value = rs.getObject(i);

                    if (value instanceof Timestamp) {
                        value = ((Timestamp) value).toLocalDateTime();
                    }

                    row.put(columName, value);
                }
                return row;
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    public <T> T selectRow(Class<T> clazz) {
        String sql = sb.toString().trim();

        showQuery();

        try {
            PreparedStatement preparedStatement = connection.prepareStatement(sql);

            ResultSet rs = preparedStatement.executeQuery();
            ResultSetMetaData metaData = rs.getMetaData();
            int count = metaData.getColumnCount();

            if (rs.next()) {
                T t = clazz.getDeclaredConstructor().newInstance();
                for (int i = 1; i <= count; i++) {
                    String columName = metaData.getColumnName(i);
                    Object value = rs.getObject(i);

                    if (value instanceof Timestamp) {
                        value = ((Timestamp) value).toLocalDateTime();
                    }

                    Field field = clazz.getDeclaredField(columName);
                    field.setAccessible(true);
                    field.set(t, value);
                }
                return t;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    public LocalDateTime selectDatetime() {
        String sql = sb.toString().trim();

        showQuery();

        try {
            PreparedStatement preparedStatement = connection.prepareStatement(sql);

            ResultSet rs = preparedStatement.executeQuery();

            if(rs.next()){
                return rs.getTimestamp(1).toLocalDateTime();
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    public Long selectLong() {
        String sql = sb.toString().trim();

        showQuery();

        try {
            PreparedStatement preparedStatement = connection.prepareStatement(sql);

            setParam(preparedStatement);

            ResultSet rs = preparedStatement.executeQuery();

            if(rs.next()){
                return rs.getLong(1);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return null;
    }


    public String selectString() {
        String sql = sb.toString().trim();

        showQuery();

        try {
            PreparedStatement preparedStatement = connection.prepareStatement(sql);

            ResultSet rs = preparedStatement.executeQuery();

            if(rs.next()){
                return rs.getString(1);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    public Boolean selectBoolean() {
        String sql = sb.toString().trim();

        showQuery();

        try {
            PreparedStatement preparedStatement = connection.prepareStatement(sql);

            ResultSet rs = preparedStatement.executeQuery();

            if(rs.next()){
                return rs.getBoolean(1);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    public List<Long> selectLongs() {
        String sql = sb.toString().trim();

        showQuery();

        try {
            PreparedStatement preparedStatement = connection.prepareStatement(sql);

            ResultSet rs = preparedStatement.executeQuery();
            List<Long> foundLongs = new ArrayList<>();

            while (rs.next()){
                foundLongs.add(rs.getLong(1));
            }

            return foundLongs;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
