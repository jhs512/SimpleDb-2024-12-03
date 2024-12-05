package com.ll;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class Sql {
    String dbUrl;
    String username;
    String password;
    String rawStmt;
    List<Object> args;
    static final ObjectMapper objectMapper = new ObjectMapper();

    static {
        objectMapper.registerModule(new JavaTimeModule());
    }

    public Sql(String dbUrl, String username, String password) {
        rawStmt = "";
        args = new ArrayList<>();
        this.dbUrl = dbUrl;
        this.username = username;
        this.password = password;
    }

    public Sql appendIn(String segment, Object... objs) {
        return appendInArray(segment, objs);
    }

    public Sql appendInArray(String segment, Object[] objs) {
        StringBuilder sb = new StringBuilder();
        String[] params = extractParentheses(segment).split(",");


        for (int i=0; i < params.length && !params[i].contains("?") ;++i) {
            sb.append(params[i].trim());
            sb.append(", ");
        }
        sb.append("?, ".repeat(objs.length));
        sb.setLength(sb.length() - 2);

        rawStmt += segment.replace(extractParentheses(segment), sb.toString()) + "\n";
        args.addAll(Arrays.asList(objs));

        return this;
    }

    private String extractParentheses(String query) {
        Pattern pattern = Pattern.compile("\\((.*?)\\)");
        Matcher matcher = pattern.matcher(query);

        if (matcher.find()) {
            return matcher.group(1); // Return the content inside parentheses
        } else {
            throw new RuntimeException("Format Error: " + query);
        }
    }

    public Sql append(String segment, Object... objs) {
        rawStmt += segment + "\n";
        args.addAll(Arrays.asList(objs));
        return this;
    }

    public long insert() {
        return update();
    }

    public long delete() {
        return update();
    }

    public long update() {
        return updateTemplate(new UpdateResultGetter<Integer>() {
            @Override
            public Integer getResult(PreparedStatement statement) throws SQLException{
                return statement.executeUpdate();
            }
        });
    }

    private int updateTemplate(UpdateResultGetter<Integer> resultGetter) {
        Connection connection = null;
        PreparedStatement statement = null;

        try {
            connection = DriverManager.getConnection(dbUrl, username, password);
            statement = connection.prepareStatement(rawStmt);

            for (int i=0; i < args.size(); ++i) {
                statement.setObject(i+1, args.get(i));
            }

            return resultGetter.getResult(statement);

        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                };
            }

            if (statement != null) {
                try {
                    connection.close();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                };
            }
        }
    }


    public Boolean selectBoolean() {
        return selectTemplate(new SelectResultGetter<Boolean>() {
            @Override
            public Boolean getResult(PreparedStatement statement, ResultSet resultSet) throws SQLException{

                resultSet = statement.executeQuery();
                if (!resultSet.next()) return null;

                return resultSet.getBoolean(1);
            }
        });
    }

    public String selectString() {
        return selectTemplate(new SelectResultGetter<String>() {
            @Override
            public String getResult(PreparedStatement statement, ResultSet resultSet) throws SQLException{

                resultSet = statement.executeQuery();
                if (!resultSet.next()) return null;

                return resultSet.getString(1);
            }
        });
    }

    public List<Long> selectLongs() {
        return selectTemplate(new SelectResultGetter<List<Long>>() {
            @Override
            public List<Long> getResult(PreparedStatement statement, ResultSet resultSet) throws SQLException{

                resultSet = statement.executeQuery();
                List<Long> rows = new ArrayList<>();

                while (resultSet.next()) {
                    rows.add(resultSet.getLong(1));
                }

                return rows;
            }
        });
    }

    public Long selectLong() {
        return selectTemplate(new SelectResultGetter<Long>() {
            @Override
            public Long getResult(PreparedStatement statement, ResultSet resultSet) throws SQLException{

                resultSet = statement.executeQuery();
                if (!resultSet.next()) return null;

                return resultSet.getLong(1);
            }
        });
    }

    public LocalDateTime selectDatetime() {
        return selectTemplate(new SelectResultGetter<LocalDateTime>() {
            @Override
            public LocalDateTime getResult(PreparedStatement statement, ResultSet resultSet) throws SQLException{
                resultSet = statement.executeQuery();
                if (!resultSet.next()) return null;
                return resultSet.getObject(1, LocalDateTime.class);
            }
        });
    }

    public <T> T selectRow(Class<T> clazz) {
        return objectMapper.convertValue(selectRow(), clazz);
    }

    public <T> List<T> selectRows(Class<T> clazz) {
        return selectRows().stream().map(m -> objectMapper.convertValue(m, clazz)).collect(Collectors.toList());
    }

    public List<Map<String, Object>> selectRows() {
        return selectTemplate(new SelectResultGetter<List<Map<String, Object>>>() {
            @Override
            public List<Map<String, Object>> getResult(PreparedStatement statement, ResultSet resultSet) throws SQLException {
                resultSet = statement.executeQuery();
                List<Map<String, Object>> rows = new ArrayList<>();

                while (resultSet.next()) {
                    rows.add(columnToMap(resultSet));
                }

                return rows;
            }
        });
    }

    public Map<String, Object> selectRow() {
        return selectTemplate(new SelectResultGetter<Map<String, Object>>() {
            @Override
            public Map<String, Object> getResult(PreparedStatement statement, ResultSet resultSet) throws SQLException{
                resultSet = statement.executeQuery();
                if (!resultSet.next()) return null;

                return columnToMap(resultSet);
            }
        });
    }

    private Map<String, Object> columnToMap(ResultSet resultSet) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        int colSize = metaData.getColumnCount();

        // columns to map
        Map<String, Object> resultMap = new HashMap<>();
        for (int i=1; i<=colSize; ++i) {
            resultMap.put(metaData.getColumnName(i), resultSet.getObject(i));
        }
        return resultMap;
    }

    private <T> T selectTemplate(SelectResultGetter<T> selectResultGetter) {
        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet resultSet = null;


        try {
            connection = DriverManager.getConnection(dbUrl, username, password);
            statement = connection.prepareStatement(rawStmt);

            for (int i=0; i < args.size(); ++i) {
                statement.setObject(i+1, args.get(i));
            }


            return selectResultGetter.getResult(statement, resultSet);

        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                };
            }

            if (statement != null) {
                try {
                    connection.close();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                };
            }
        }

    }

}
