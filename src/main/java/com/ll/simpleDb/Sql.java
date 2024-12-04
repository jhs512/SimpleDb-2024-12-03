package com.ll.simpleDb;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

public class Sql {

    public Connection connection;
    public Sql(Connection connection) {
        this.connection = connection;
    }
    private final StringBuilder query = new StringBuilder();
    private final ArrayList<Object> params = new ArrayList<>();

    public Sql append(String str, Object... values) {
        query.append(str).append(" ");
        // Object 타입으로 값을 추가
        params.addAll(Arrays.asList(values));
        return this;
    }

    public Sql appendIn(String s, Object... values) {
        String[] split = s.split("\\?");
        String sb = split[0] + "?"
            + ", ?".repeat(Math.max(0, values.length - 1)) + split[split.length - 1];

        query.append(sb);
        params.addAll(Arrays.asList(values));
        return this;
    }

    public long insert() {
        long articleId = -1L;
        try {
            PreparedStatement statement = connection.prepareStatement(query.toString(),
                PreparedStatement.RETURN_GENERATED_KEYS);

            IntStream.range(0, params.size())
                .forEach(i -> {
                    try {
                        statement.setString(i + 1, (String) params.get(i));
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                });

            int result = statement.executeUpdate();
            if(result > 0) {
                System.out.println("Create Article success");
                ResultSet generatedKeys = statement.getGeneratedKeys();
                if(generatedKeys.next()) {
                    articleId = generatedKeys.getLong(1);
                }
            }

            statement.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return articleId;
    }

    public long update() {
        return executeUpdateQuery();
    }

    public long delete() {
        return executeUpdateQuery();
    }

    private long executeUpdateQuery() {
        long result;
        try {
            PreparedStatement statement = connection.prepareStatement(query.toString());

            IntStream.range(0, params.size())
                .forEach(i -> {
                    try {
                        if (params.get(i) instanceof Integer) {
                            statement.setInt(i + 1, (Integer) params.get(i));
                        } else {
                            statement.setString(i + 1, (String) params.get(i));
                        }
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                });

            result = statement.executeUpdate();
            statement.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return result;
    }

    public List<Map<String, Object>> selectRows() {
        List<Map<String, Object>> rows = new ArrayList<>();
        try {
            PreparedStatement statement = connection.prepareStatement(query.toString());
            ResultSet resultSet = statement.executeQuery();

            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();
            while (resultSet.next()) {
                Map<String, Object> row = new HashMap<>();

                for (int i = 1; i <= columnCount; i++) {
                    row.put(metaData.getColumnName(i), resultSet.getObject(i));
                    System.out.println(resultSet.getObject(i));
                }
                rows.add(row);
            }
            statement.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return rows;
    }

    public List<Article> selectRows(Class<Article> articleClass) {
        List<Article> articles = new ArrayList<>();
        try {
            PreparedStatement statement = connection.prepareStatement(query.toString());
            ResultSet resultSet = statement.executeQuery();

            while (resultSet.next()) {
                Article article = new Article(
                    resultSet.getLong("id"),
                    resultSet.getString("title"),
                    resultSet.getString("body"),
                    resultSet.getBoolean("isBlind"),
                    resultSet.getTimestamp("createdDate").toLocalDateTime(),
                    resultSet.getTimestamp("modifiedDate").toLocalDateTime()
                );
                articles.add(article);
            }
            statement.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return articles;
    }


    public LocalDateTime selectDatetime() {
        return selectValue("now()", LocalDateTime.class);
    }

    public Long selectLong() {
        if(query.toString().contains("?")) {
            return executeQuery();
        }
        return selectValue("id", Long.class);
    }

    private long executeQuery() {
        long result = 0;
        try {
            PreparedStatement statement = connection.prepareStatement(query.toString());

            IntStream.range(0, params.size())
                .forEach(i -> {
                    try {
                        if (params.get(i) instanceof Integer) {
                            statement.setInt(i + 1, (Integer) params.get(i));
                        } else {
                            statement.setString(i + 1, (String) params.get(i));
                        }
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                });

            ResultSet resultSet = statement.executeQuery();

            String substring = query.substring(7);
            String[] split = substring.split(" ");
            if (resultSet.next()) {
                result = resultSet.getLong(split[0]);
            }
            statement.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return result;
    }

    private List<Long> executeQueryForList() {
        List<Long> result = new ArrayList<>();
        try {
            PreparedStatement statement = connection.prepareStatement(query.toString());

            IntStream.range(0, params.size())
                .forEach(i -> {
                    try {
                        if (params.get(i) instanceof Integer) {
                            statement.setInt(i + 1, (Integer) params.get(i));
                        } else if (params.get(i) instanceof Long) {
                            statement.setLong(i + 1, (Long) params.get(i));
                        } else {
                            statement.setString(i + 1, (String) params.get(i));
                        }
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                });

            ResultSet resultSet = statement.executeQuery();
            while(resultSet.next()) {
                result.add(resultSet.getLong(1));
            }

            statement.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return result;
    }

    public String selectString() {
        return selectValue("title", String.class);
    }

    private <T> T selectValue(String columnName, Class<T> type) {
        T result = null;
        try {
            PreparedStatement statement = connection.prepareStatement(query.toString());
            ResultSet resultSet = statement.executeQuery();
            if (resultSet.next()) {
                if(type == Long.class) {
                    result = type.cast(resultSet.getLong(columnName));
                } else if(type == String.class) {
                    result = type.cast(resultSet.getString(columnName));
                } else if(type == LocalDateTime.class) {
                    result = type.cast(resultSet.getTimestamp(columnName).toLocalDateTime());
                } else if (type == Boolean.class) {
                    try {
                        result = type.cast(resultSet.getBoolean(columnName));
                    } catch (SQLException e) {
                        String substring = query.substring(7).trim();
                        result = type.cast(resultSet.getBoolean(substring));
                    }
                }
            }
            statement.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    public Boolean selectBoolean() {
        return selectValue("isBlind", Boolean.class);
    }

    public List<Long> selectLongs() {
        return executeQueryForList();
    }
}
