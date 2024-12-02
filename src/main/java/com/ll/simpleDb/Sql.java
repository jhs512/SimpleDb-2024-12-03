package com.ll.simpleDb;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.stream.IntStream;

public class Sql {

    public Connection connection = null;
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
}
