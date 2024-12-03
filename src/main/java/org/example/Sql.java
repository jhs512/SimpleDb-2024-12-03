package org.example;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;

public class Sql {
    private StringBuilder sql = new StringBuilder();
    private SimpleDb simpleDb;
    private final Connection connection;
    private PreparedStatement preparedStatement;
    private List<Object> params = new ArrayList<>();

    public Sql(SimpleDb db) {
        simpleDb = db;
        this.connection = db.getConnection();
    }

    public Sql append(String sql, Object... params) {
        this.sql.append(sql).append("\n");
        this.params.addAll(Arrays.asList(params));
        return this;
    }

    public Sql appendIn(String sql, Object... params) {
        StringBuilder parsedParam = new StringBuilder();
        for (int i = 0; i < params.length; i++) {
            parsedParam.append("'").append(params[i]).append("'");
            if(i != params.length - 1) parsedParam.append(",");
        }
        sql = sql.replaceFirst("\\?", parsedParam.toString());

        this.sql.append(sql).append("\n");
        return this;
    }


    public long insert() {
        long id = -1;
        try {
            preparedStatement = connection.prepareStatement(sql.toString(), Statement.RETURN_GENERATED_KEYS);
            for(int i=0; i<params.size(); i++) {
                preparedStatement.setObject(i+1, params.get(i));
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

    public long update() {
        long affectedRows = excuteUpdateSql();

        return affectedRows;
    }

    public long delete() {
        long affectedRows = excuteUpdateSql();

        return affectedRows;
    }

    public List<Map<String, Object>> selectRows() {
        List<Map<String, Object>> articles = new ArrayList<>();

        try {
            ResultSet rs = selectExcute();

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
            ResultSet rs = selectExcute();

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
        Map<String, Object> article = new HashMap<>();

        try {
            ResultSet rs = selectExcute();
            rs.next();

            article.put("id", rs.getLong("id"));
            article.put("createdDate", rs.getTimestamp("createdDate").toLocalDateTime());
            article.put("modifiedDate", rs.getTimestamp("modifiedDate").toLocalDateTime());
            article.put("title", rs.getString("title"));
            article.put("body", rs.getString("body"));
            article.put("isBlind", rs.getBoolean("isBlind"));

            preparedStatement.close();
            rs.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return article;
    }

    public LocalDateTime selectDatetime() {
        LocalDateTime localDateTime;
        try {
            ResultSet rs = selectExcute();
            rs.next();
            localDateTime = rs.getTimestamp("NOW()").toLocalDateTime();

            preparedStatement.close();
            rs.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return localDateTime;
    }

    public Long selectLong() {
        long count;
        try {
            ResultSet rs = selectExcute();
            rs.next();
            count = rs.getLong(rs.getMetaData().getColumnName(1));

            preparedStatement.close();
            rs.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return count;
    }

    public List<Long> selectLongs() {
        List<Long> longs = new ArrayList<>();

        try {
            ResultSet rs = selectExcute();
            ResultSetMetaData rsmd = rs.getMetaData();

            while(rs.next()) {
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
        String title;
        try {
            ResultSet rs = selectExcute();
            rs.next();
            title = rs.getString("title");

            preparedStatement.close();
            rs.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return title;
    }

    public Boolean selectBoolean() {
        Boolean isBlind;
        try {
            ResultSet rs = selectExcute();
            rs.next();
            isBlind = rs.getBoolean(rs.getMetaData().getColumnName(1));

            preparedStatement.close();
            rs.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return isBlind;
    }

    private ResultSet selectExcute() throws SQLException {
        preparedStatement = connection.prepareStatement(sql.toString());
        for(int i=0; i<params.size(); i++) {
            preparedStatement.setObject(i+1, params.get(i));
        }
        ResultSet rs = preparedStatement.executeQuery();

        showSql();
        return rs;
    }

    private long excuteUpdateSql() {
        long affectedRows = 0;
        try {
            preparedStatement = connection.prepareStatement(sql.toString());
            for(int i=0; i<params.size(); i++) {
                preparedStatement.setObject(i+1, params.get(i));
            }
            affectedRows = preparedStatement.executeUpdate();

            preparedStatement.close();
        } catch (SQLException e) {
            System.out.println("sql 입력 오류 = " + e);
        }

        showSql();
        return affectedRows;
    }


    private void showSql() {
        if (simpleDb.getDevMode()) {
            System.out.println("== rawSql ==");
            System.out.println("sql = " + sql);
        }
    }



}
