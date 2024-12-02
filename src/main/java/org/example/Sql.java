package org.example;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;

public class Sql {
    private final String host;
    private final String username;
    private final String password;
    private StringBuilder sb;
    private List<Object> params;

    public Sql(String host, String username, String password) {
        this.host = host;
        this.username = username;
        this.password = password;
        this.sb = new StringBuilder();
        this.params = new ArrayList<>();
    }

    public Sql append(String sql, Object... args) {
        sb.append(sql).append(" ");
        params.addAll(Arrays.asList(args));
        return this;
    }

    public Long insert() {
        String sql = sb.toString().trim();

        try {
            Connection connection = DriverManager.getConnection(host, username, password);
            PreparedStatement preparedStatement = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);

            for (int i = 0; i < params.size(); i++) {
                preparedStatement.setObject(i + 1, params.get(i));
            }

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

        try {
            Connection connection = DriverManager.getConnection(host, username, password);
            PreparedStatement preparedStatement = connection.prepareStatement(sql);

            for (int i = 0; i < params.size(); i++) {
                preparedStatement.setObject(i + 1, params.get(i));
            }

            return preparedStatement.executeUpdate();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


    public long delete() {
        String sql = sb.toString().trim();

        try {
            Connection connection = DriverManager.getConnection(host, username, password);
            PreparedStatement preparedStatement = connection.prepareStatement(sql);

            for (int i = 0; i < params.size(); i++) {
                preparedStatement.setObject(i + 1, params.get(i));
            }

            return preparedStatement.executeUpdate();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public List<Map<String, Object>> selectRows() {
        String sql = sb.toString().trim();

        try {
            Connection connection = DriverManager.getConnection(host, username, password);
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

    public Map<String, Object> selectRow() {
        String sql = sb.toString().trim();

        try {
            Connection connection = DriverManager.getConnection(host, username, password);
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

    public LocalDateTime selectDatetime() {
        String sql = sb.toString().trim();

        try {
            Connection connection = DriverManager.getConnection(host, username, password);
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
}
