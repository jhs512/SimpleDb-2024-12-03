package org.example;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class Sql {
    private String query = "";
    private List<Object> params = new ArrayList<>();
    private Connection conn;

    public Sql(Connection conn) {
        this.conn = conn;
    }

    public Sql append(String query) {
        this.query += query + " ";
        return this;
    }

    public Sql append(String query, Object... params) {
        this.query += query + " ";
        for (int i = 0; i < params.length; i++) {
            this.params.add(params[i]);
        }

        return this;
    }

    public void run(String query) {
        try {
            Statement stmt = conn.createStatement();
            stmt.executeUpdate(query);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public long insert() {
        try {
            PreparedStatement pstmt = conn.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);

            for (int i = 0; i < params.size(); i++) {
                pstmt.setObject(i + 1, params.get(i));
            }

            params.clear();
            query = "";
            pstmt.executeUpdate();

            try (ResultSet generatedKeys = pstmt.getGeneratedKeys()) {
                if (generatedKeys.next()) {
                    return generatedKeys.getLong(1);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return -1;
    }

    public long update() {
        try {
            PreparedStatement pstmt = conn.prepareStatement(query);

            for (int i = 0; i < params.size(); i++) {
                pstmt.setObject(i + 1, params.get(i));
            }

            params.clear();
            query = "";
            return pstmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return -1;
    }

    @Override
    public String toString() {
        return query;
    }
}
