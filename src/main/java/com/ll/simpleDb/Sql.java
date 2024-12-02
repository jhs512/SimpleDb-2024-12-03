package com.ll.simpleDb;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
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
    private final ArrayList<String> params = new ArrayList<>();

    public Sql append(String str) {
        query.append(str);
        return this;
    }

    public Sql append(String str, String value) {
        query.append(str);
        params.add(value);
        return this;
    }

    public Sql append(String str, int value, int value2, int value3) {
        query.append(str);
        params.add(value+"");
        params.add(value2+"");
        params.add(value3+"");
        return this;
    }

    public Sql append(String str, int value, int value2, int value3, int value4) {
        query.append(str);
        params.add(value+"");
        params.add(value2+"");
        params.add(value3+"");
        params.add(value4+"");
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
                        statement.setString(i + 1, params.get(i));
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
                        if (isNumber(params.get(i))) {
                            statement.setInt(i + 1, Integer.parseInt(params.get(i)));
                        } else {
                            statement.setString(i + 1, params.get(i));
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

    private boolean isNumber(String target) {
        try {
            Integer.parseInt(target);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
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


    public LocalDateTime selectDatetime() {
        LocalDateTime result = null;
        try {
            PreparedStatement statement = connection.prepareStatement(query.toString());
            ResultSet resultSet = statement.executeQuery();
            if(resultSet.next()) {
                result = LocalDateTime.parse(resultSet.getString(1).replace(" ", "T"));
            }
            statement.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return result;
    }

    public Long selectLong() {
        long result = 0L;
        try {
            PreparedStatement statement = connection.prepareStatement(query.toString());
            ResultSet resultSet = statement.executeQuery();
            if (resultSet.next()) {
                result = resultSet.getLong("id");
            }
            statement.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return result;
    }

}
