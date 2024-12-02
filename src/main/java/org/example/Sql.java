package org.example;

import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateDeserializer;

import java.sql.*;
import java.time.LocalDateTime;
import java.time.chrono.ChronoLocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Sql {
    private StringBuilder sql = new StringBuilder();
    private SimpleDb simpleDb;
    private final Connection connection;
    private PreparedStatement preparedStatement;

    public Sql(SimpleDb db) {
        simpleDb = db;
        this.connection = db.getConnection();
    }

    public Sql append(String sql) {
        this.sql.append(sql).append("\n");
        return this;
    }

    public Sql append(String sql, String param) {
        sql = sql.replace("?", "\"" + param + "\"");
        this.sql.append(sql).append("\n");
        return this;
    }

    public Sql append(String sql, int... params) {
        for (int param : params) {
            sql = sql.replaceFirst("\\?", param + "");
            System.out.println("sql = " + sql);
        }
        this.sql.append(sql).append("\n");
        return this;
    }


    public long insert() {
        long id = -1;
        try {
            preparedStatement = connection.prepareStatement(sql.toString(), Statement.RETURN_GENERATED_KEYS);
            preparedStatement.executeUpdate();
            ResultSet generatedKey = preparedStatement.getGeneratedKeys();
            if (generatedKey.next()) {
                id = generatedKey.getLong(1);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        showSql();
        return id;
    }

    public long update() {
        long affectedRows = excuteUpdateSql();
        showSql();
        return affectedRows;
    }

    public long delete() {
        long affectedRows = excuteUpdateSql();
        showSql();
        return affectedRows;
    }

    public List<Map<String, Object>> selectRows() {
        List<Map<String, Object>> articles = new ArrayList<>();

        try {
            preparedStatement = connection.prepareStatement(sql.toString());

            ResultSet rs = preparedStatement.executeQuery();
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
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        showSql();
        return articles;
    }

    public Map<String, Object> selectRow() {
        Map<String, Object> article = new HashMap<>();
        try {
            preparedStatement = connection.prepareStatement(sql.toString());
            ResultSet rs = preparedStatement.executeQuery();

            while(rs.next()) {
                article.put("id", rs.getLong("id"));
                article.put("createdDate", rs.getTimestamp("createdDate").toLocalDateTime());
                article.put("modifiedDate", rs.getTimestamp("modifiedDate").toLocalDateTime());
                article.put("title", rs.getString("title"));
                article.put("body", rs.getString("body"));
                article.put("isBlind", rs.getBoolean("isBlind"));
            }


        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        showSql();
        return article;
    }

    public LocalDateTime selectDatetime() {

        return null;
    }

    private long excuteUpdateSql() {
        long affectedRows = 0;
        try {
            preparedStatement = connection.prepareStatement(sql.toString());
            affectedRows = preparedStatement.executeUpdate();
        } catch (SQLException e) {
            System.out.println("sql 입력 오류 = " + e);
        }
        return affectedRows;
    }


    private void showSql() {
        if (simpleDb.getDevMode()) {
            System.out.println("== rawSql ==");
            System.out.println("sql = " + sql);
        }
    }


}
