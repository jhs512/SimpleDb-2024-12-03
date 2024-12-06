package org.example;

import lombok.Setter;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SimpleDb {
    private String username;
    private String password;
    private String url;
    @Setter
    private Boolean devMode = false;
    @Setter
    private String sql;
    @Setter
    private Object[] params;
    private final ThreadLocal<Connection> threadLocalConnection = ThreadLocal.withInitial(() -> {
        try {
            return DriverManager.getConnection(url, username, password);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    });

    public SimpleDb(String host, String username, String password, String dbName) {
        int port = 3306;

        this.url = "jdbc:mysql://" + host + ":" + port + "/" + dbName;
        this.username = username;
        this.password = password;
    }

    public Object run(String sql, Object... params) {
        Object result = null;
        try {
            Connection connection = threadLocalConnection.get();
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            for (int i = 0; i < params.length; i++) {
                preparedStatement.setObject(i + 1, params[i]);
            }
            if(sql.startsWith("SELECT")) {
                result = preparedStatement.executeQuery();
            }
            else {
                result = preparedStatement.executeUpdate();
            }

        } catch (SQLException e) {
            System.out.println("SimpleDb단 sql 오류 " + e);
        }

        return result;
    }

    public Sql genSql() {

        return new Sql(this);
    }

    public void closeConnection() {
        try {
            Connection connection = threadLocalConnection.get();
            if (connection != null && !connection.isClosed()) {
                connection.close();
                threadLocalConnection.remove();
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to close connection", e);
        }
    }

    public void startTransaction() {
        try{
            Connection connection = threadLocalConnection.get();
            connection.setAutoCommit(false);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void rollback() {
        try {
            Connection connection = threadLocalConnection.get();
            connection.rollback();
            connection.setAutoCommit(true);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void commit() {
        try {
            Connection connection = threadLocalConnection.get();
            connection.commit();
            connection.setAutoCommit(true);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void  setSqlAndParams(String sql, Object[] params) {
        this.sql = sql;
        this.params = params;
    }

    public long insert() {
        long id = -1;
        try {
            Connection connection = threadLocalConnection.get();
            PreparedStatement preparedStatement = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            for (int i = 0; i < params.length; i++) {
                preparedStatement.setObject(i + 1, params[i]);
            }
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

    public int update() {

        return executeUpdate();
    }

    public int delete() {

        return executeUpdate();
    }

    public List<Map<String, Object>> selectRows() {
        List<Map<String, Object>> articles = new ArrayList<>();

        try(ResultSet rs = executeSelect()) {

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

        return articles;
    }

    public List<Article> selectRows(Class<Article> articleClass) {
        List<Article> articles = new ArrayList<>();
        try(ResultSet rs = executeSelect()) {

            while (rs.next()) {
                articles.add(new Article(rs.getLong("id"),
                        rs.getString("title"),
                        rs.getString("body"),
                        rs.getTimestamp("createdDate").toLocalDateTime(),
                        rs.getTimestamp("modifiedDate").toLocalDateTime(),
                        rs.getBoolean("isBlind")));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return articles;
    }

    public LocalDateTime selectDatetime() {

        return (LocalDateTime) selectOneRowOneColumn();
    }

    public Long selectLong() {

        return (long) selectOneRowOneColumn();
    }

    public List<Long> selectLongs() {
        List<Long> longs = new ArrayList<>();

        try(ResultSet rs = executeSelect()) {

            ResultSetMetaData rsmd = rs.getMetaData();

            while (rs.next()) {
                longs.add(rs.getLong(rsmd.getColumnName(1)));
            }

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
        try(ResultSet rs = executeSelect()) {
            rs.next();

            result = rs.getObject(1);

        } catch (SQLException e) {
            System.out.println("Sql단 오류 = " + e);
        }

        return result;
    }

    private ResultSet executeSelect() throws SQLException {
        showSql();
        return (ResultSet) run(sql, params);
    }

    private int executeUpdate() {
        showSql();
        return (int) run(sql, params);
    }

    private void showSql() {
        if (devMode) {
            System.out.println("== rawSql ==");
            System.out.println(sql);
        }
    }
}
