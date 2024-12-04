package org.example;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;

public class Sql {
    private StringBuilder sql = new StringBuilder();
    private final Connection connection;
    private PreparedStatement preparedStatement;
    private final List<Object> params = new ArrayList<>();
    private final Boolean devMode;

    public Sql(Boolean devMode, Connection connection) {
        this.devMode = devMode;
        this.connection = connection;
    }

    public Sql append(String sql, Object... params) {
        this.sql.append(sql).append("\n");
        this.params.addAll(Arrays.asList(params));
        return this;
    }

    public Sql appendIn(String sql, Object... params) {
        StringBuilder parsedParam = new StringBuilder();

        // MySQL은 PreparedStatement.setArray 메서드를 지원하지 않기 때문에 직접 문자열로 파싱
        for (int i = 0; i < params.length; i++) {
            parsedParam.append("'").append(params[i]).append("'");
            if (i != params.length - 1) parsedParam.append(",");
        }
        sql = sql.replaceFirst("\\?", parsedParam.toString());

        this.sql.append(sql).append("\n");
        return this;
    }


    public long insert() {
        long id = -1;
        try {
            preparedStatement = connection.prepareStatement(sql.toString(), Statement.RETURN_GENERATED_KEYS);
            for (int i = 0; i < params.size(); i++) {
                preparedStatement.setObject(i + 1, params.get(i));
            }
            preparedStatement.executeUpdate();

            ResultSet generatedKey = preparedStatement.getGeneratedKeys();
            if (generatedKey.next()) {
                id = generatedKey.getLong(1);
            }

            preparedStatement.close();
            generatedKey.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        showSql();
        return id;
    }

    public int update() {

        return excuteUpdate();
    }

    public int delete() {

        return excuteUpdate();
    }

    public List<Map<String, Object>> selectRows() {
        List<Map<String, Object>> articles = new ArrayList<>();

        try {
            ResultSet rs = excuteSelect();

            while (rs.next()) {
                Map<String, Object> articleMap = new HashMap<>();
                articleMap.put("id", rs.getLong("id"));
                articleMap.put("createdDate", rs.getTimestamp("createdDate").toLocalDateTime());
                articleMap.put("modifiedDate", rs.getTimestamp("modifiedDate").toLocalDateTime());
                articleMap.put("title", rs.getString("title"));
                articleMap.put("body", rs.getString("body"));
                articleMap.put("isBlind", rs.getBoolean("isBlind"));
                articles.add(articleMap); // 맵을 리스트에 추가
            }

            preparedStatement.close();
            rs.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return articles;
    }

    public List<Article> selectRows(Class<Article> articleClass) {
        List<Article> articles = new ArrayList<>();
        try {
            ResultSet rs = excuteSelect();

            while (rs.next()) {
                articles.add(new Article(rs.getLong("id"),
                        rs.getString("title"),
                        rs.getString("body"),
                        rs.getTimestamp("createdDate").toLocalDateTime(),
                        rs.getTimestamp("modifiedDate").toLocalDateTime(),
                        rs.getBoolean("isBlind")));
            }

            preparedStatement.close();
            rs.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return articles;
    }

    public Map<String, Object> selectRow() {

        return selectRows().getFirst();
    }

    public Article selectRow(Class<Article> articleClass) {

        return selectRows(Article.class).getFirst();
    }

    public LocalDateTime selectDatetime() {

        return (LocalDateTime) selectOneRowOneColumn();
    }

    public Long selectLong() {

        return (long) selectOneRowOneColumn();
    }

    public List<Long> selectLongs() {
        List<Long> longs = new ArrayList<>();

        try {
            ResultSet rs = excuteSelect();
            ResultSetMetaData rsmd = rs.getMetaData();

            while (rs.next()) {
                longs.add(rs.getLong(rsmd.getColumnName(1)));
            }

            preparedStatement.close();
            rs.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return longs;
    }

    public String selectString() {

        return selectOneRowOneColumn().toString();
    }

    public Boolean selectBoolean() {
        // toString적용 시 "1" or "0"으로 반환됨
        return selectOneRowOneColumn().toString().equals("1");
    }

    private Object selectOneRowOneColumn() {
        Object result = null;
        try {
            ResultSet rs = excuteSelect();
            rs.next();

            result = rs.getObject(1);

            preparedStatement.close();
            rs.close();
            connection.close();
        } catch (SQLException e) {
            System.out.println("Sql단 오류 = " + e);
        }

        return result;
    }

    private ResultSet excuteSelect() throws SQLException {
        preparedStatement = connection.prepareStatement(sql.toString());
        for (int i = 0; i < params.size(); i++) {
            preparedStatement.setObject(i + 1, params.get(i));
        }
        ResultSet rs = preparedStatement.executeQuery();

        showSql();
        return rs;
    }

    private int excuteUpdate() {
        int affectedRows = 0;
        try {
            preparedStatement = connection.prepareStatement(sql.toString());
            for (int i = 0; i < params.size(); i++) {
                preparedStatement.setObject(i + 1, params.get(i));
            }
            affectedRows = preparedStatement.executeUpdate();
            preparedStatement.close();
        } catch (SQLException e) {
            System.out.println("Sql단 오류 = " + e);
        }

        showSql();
        return affectedRows;
    }


    private void showSql() {
        if (devMode) {
            System.out.println("== rawSql ==");
            System.out.println(sql);
        }
    }


}