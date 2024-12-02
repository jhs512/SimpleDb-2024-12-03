package org.example;

import java.sql.*;

public class Sql {
    private String query = "";
    private Connection conn;

    public Sql(Connection conn) {
        this.conn = conn;
    }

    public Sql append(String query) {
        this.query += query + " ";
        return this;
    }

    public Sql append(String query, String param) {
        this.query += query.replace("?", "'" + param + "'");
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

    @Override
    public String toString() {
        return query;
    }
}
