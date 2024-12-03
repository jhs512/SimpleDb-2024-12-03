package org.example;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;

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
        this.params.addAll(Arrays.asList(params));

        return this;
    }

    public void run(String query, Object...params) {
        try {
            PreparedStatement pstmt = conn.prepareStatement(query);
            this.params.addAll(Arrays.asList(params));

            setParams(pstmt);
            clearQuery();

            pstmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public long insert() {
        try {
            PreparedStatement pstmt = conn.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
            setParams(pstmt);
            clearQuery();

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
            setParams(pstmt);
            clearQuery();

            return pstmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return -1;
    }

    public long delete() {
        try {
            PreparedStatement pstmt = conn.prepareStatement(query);
            setParams(pstmt);
            clearQuery();

            return pstmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return -1;
    }

    public List<Map<String, Object>> selectRows() {
        try {
            PreparedStatement pstmt = conn.prepareStatement(query);
            setParams(pstmt);
            clearQuery();

            ResultSet rs = pstmt.executeQuery();
            List<Map<String, Object>> results = new ArrayList<>();
            while (rs.next()) {
                Map<String, Object> result = new HashMap<>();
                result.put("id", rs.getLong("id"));
                result.put("title", rs.getString("title"));
                result.put("body", rs.getString("body"));
                result.put("createdDate", rs.getTimestamp("createdDate").toLocalDateTime());
                result.put("modifiedDate", rs.getTimestamp("modifiedDate").toLocalDateTime());
                result.put("isBlind", rs.getBoolean("isBlind"));

                results.add(result);
            }

            return results;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public Map<String, Object> selectRow() {
        try {
            PreparedStatement pstmt = conn.prepareStatement(query);
            setParams(pstmt);
            clearQuery();

            ResultSet rs = pstmt.executeQuery();
            Map<String, Object> result = new HashMap<>();
            if (rs.next()) {
                result.put("id", rs.getLong("id"));
                result.put("title", rs.getString("title"));
                result.put("body", rs.getString("body"));
                result.put("createdDate", rs.getTimestamp("createdDate").toLocalDateTime());
                result.put("modifiedDate", rs.getTimestamp("modifiedDate").toLocalDateTime());
                result.put("isBlind", rs.getBoolean("isBlind"));
            }

            return result;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public LocalDateTime selectDatetime() {
        return selectValue(query, resultSet ->
                resultSet.getTimestamp(1).toLocalDateTime()
        );
    }

    public Long selectLong() {
        return selectValue(query, resultSet ->
            resultSet.getLong(1)
        );
    }

    @Override
    public String toString() {
        return query;
    }

    private <T> T selectValue(String query, ResultSetExtractor<T> extractor) {
        try {
            PreparedStatement pstmt = conn.prepareStatement(query);
            setParams(pstmt);
            clearQuery();

            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                try {
                    return extractor.extract(rs);
                } catch (SQLException e) {

                }

            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    private void setParams(PreparedStatement pstmt) throws SQLException {
        for (int i = 0; i < params.size(); i++) {
            pstmt.setObject(i + 1, params.get(i));
        }
    }

    private void clearQuery() {
        query = "";
        params.clear();
    }

    @FunctionalInterface
    private interface ResultSetExtractor<T> {
        T extract(ResultSet rs) throws SQLException;
    }
}
