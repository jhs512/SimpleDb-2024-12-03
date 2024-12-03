package com.ll;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.sql.*;
import java.util.*;

//prepared Statment creator
//templates 종료: update select 등등

public class Sql {
    String dbUrl;
    String username;
    String password;
    String rawStmt;
    List<Object> args;
    static ObjectMapper objectMapper = new ObjectMapper();

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
        Connection connection = null;
        PreparedStatement statement = null;
        long id;

        try {
            connection = DriverManager.getConnection(dbUrl, username, password);
            statement = connection.prepareStatement(rawStmt);

            for (int i=0; i < args.size(); ++i) {
                statement.setObject(i+1, args.get(i));
            }

            id = statement.executeUpdate();

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

        return id;
    }

    public <T> T selectRow(Class<T> clazz) {
        return objectMapper.convertValue(selectRow(), clazz);
    }

    public HashMap<String, Object> selectRow() {
        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet resultSet = null;


        try {
            connection = DriverManager.getConnection(dbUrl, username, password);
            statement = connection.prepareStatement(rawStmt);

            for (int i=0; i < args.size(); ++i) {
                statement.setObject(i+1, args.get(i));
            }

            resultSet = statement.executeQuery();
            if (!resultSet.next()) return null;

            ResultSetMetaData metaData = resultSet.getMetaData();
            int colSize = metaData.getColumnCount();

            // columns to Object
            HashMap<String, Object> resultMap = new HashMap<>();
            for (int i=1; i<=colSize; ++i) {
                resultMap.put(metaData.getColumnName(i), resultSet.getObject(i));
            }

            return resultMap;


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
