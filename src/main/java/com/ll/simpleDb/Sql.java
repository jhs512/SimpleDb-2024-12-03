package com.ll.simpleDb;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * packageName  : com.ll.simpleDb
 * fileName     : Sql
 * author       : Author
 * date         : 2024-12-03
 * description  :
 * ====================================================================================================
 * DATE           AUTHOR              NOTE
 * ----------------------------------------------------------------------------------------------------
 * 2024-12-03     Author              Initial creation.
 */
public class Sql {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final List<Object> sqlParams    = new ArrayList<>();

    private final Connection conn;
    private final Runnable   runnable;

    private String sql = "";

    public Sql(final Connection conn, final Runnable runnable) {
        this.conn = conn;
        this.runnable = runnable;
        objectMapper.registerModule(new JavaTimeModule());
    }

    public void close() {
        if (runnable != null) runnable.run();
    }

    private void clear() {
        sql = "";
        sqlParams.clear();
    }

    public Sql append(final String sql, final Object... params) {
        if (this.sql.isEmpty()) this.sql = sql;
        else this.sql += " %s".formatted(sql);
        if (params.length > 0) sqlParams.addAll(Arrays.asList(params));
        return this;
    }

    public Sql appendIn(final String sql, final Object... params) {
        if (this.sql.isEmpty()) this.sql = sql;
        else if (params.length == 0) this.sql += " %s".formatted(sql);
        else {
            List<Object> list = new ArrayList<>();
            for (Object param : params)
                if (param.getClass().isArray()) {
                    if (!list.isEmpty()) addInSqlByList(sql, list);
                    String placeholders = String.join(",", Collections.nCopies(((Object[]) param).length, "?"));
                    this.sql += " %s".formatted(sql.replace("?", placeholders));
                    sqlParams.addAll(Arrays.asList((Object[]) param));
                } else list.add(param);
            if (!list.isEmpty()) addInSqlByList(sql, list);
        }
        return this;
    }

    private void addInSqlByList(final String sql, final List<Object> list) {
        String placeholders = String.join(",", Collections.nCopies(list.size(), "?"));
        this.sql += " %s".formatted(sql.replace("?", placeholders));
        sqlParams.addAll(list);
        list.clear();
    }

    public long insert() {
        try (PreparedStatement pstmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            setParams(pstmt, sqlParams);
            pstmt.executeUpdate();
            try (ResultSet rs = pstmt.getGeneratedKeys()) {
                if (rs.next())
                    return rs.getLong(1);
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to execute insert sql: " + sql, e);
        } finally {
            clear();
            close();
        }
        return -1;
    }

    public int update() {
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            setParams(pstmt, sqlParams);
            return pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to execute update sql: " + sql, e);
        } finally {
            clear();
            close();
        }
    }

    public int delete() {
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            setParams(pstmt, sqlParams);
            return pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to execute delete sql: " + sql, e);
        } finally {
            clear();
            close();
        }
    }

    public List<Map<String, Object>> selectRows() {
        List<Map<String, Object>> rows = new ArrayList<>();
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            setParams(pstmt, sqlParams);
            try (ResultSet rs = pstmt.executeQuery()) {
                ResultSetMetaData meta        = rs.getMetaData();
                int               columnCount = meta.getColumnCount();
                while (rs.next()) {
                    Map<String, Object> row = new HashMap<>();
                    for (int i = 1; i <= columnCount; i++)
                        row.put(meta.getColumnName(i),
                                (rs.getObject(i) instanceof Timestamp)
                                ? ((Timestamp) rs.getObject(i)).toLocalDateTime()
                                : rs.getObject(i));
                    rows.add(row);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to execute select rows sql: " + sql, e);
        } finally {
            clear();
            close();
        }
        return rows;
    }

    public Map<String, Object> selectRow() {
        Map<String, Object> row = new HashMap<>();
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            setParams(pstmt, sqlParams);
            try (ResultSet rs = pstmt.executeQuery()) {
                ResultSetMetaData meta        = rs.getMetaData();
                int               columnCount = meta.getColumnCount();
                if (rs.next())
                    for (int i = 1; i <= columnCount; i++)
                        row.put(meta.getColumnName(i),
                                (rs.getObject(i) instanceof Timestamp)
                                ? ((Timestamp) rs.getObject(i)).toLocalDateTime()
                                : rs.getObject(i));
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to execute select rows sql: " + sql, e);
        } finally {
            clear();
            close();
        }
        return row;
    }

    public LocalDateTime selectDatetime() {
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            setParams(pstmt, sqlParams);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next())
                    return rs.getTimestamp(1).toLocalDateTime();
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to execute select datetime sql: " + sql, e);
        } finally {
            clear();
            close();
        }
        return null;
    }

    public Long selectLong() {
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            setParams(pstmt, sqlParams);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next())
                    return rs.getLong(1);
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to execute select long sql: " + sql, e);
        } finally {
            clear();
            close();
        }
        return null;
    }

    public String selectString() {
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            setParams(pstmt, sqlParams);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next())
                    return rs.getString(1);
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to execute select string sql: " + sql, e);
        } finally {
            clear();
            close();
        }
        return null;
    }

    public Boolean selectBoolean() {
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            setParams(pstmt, sqlParams);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next())
                    return rs.getBoolean(1);
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to execute select boolean sql: " + sql, e);
        } finally {
            clear();
            close();
        }
        return null;
    }

    public List<Long> selectLongs() {
        List<Long> rows = new ArrayList<>();
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            setParams(pstmt, sqlParams);
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next())
                    rows.add(rs.getLong(1));
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to execute select longs sql: " + sql, e);
        } finally {
            clear();
            close();
        }
        return rows;
    }

    public <T> List<T> selectRows(final Class<T> clazz) {
        List<T> rows = new ArrayList<>();
        for (Map<String, Object> row : selectRows())
            rows.add(objectMapper.convertValue(row, clazz));
        return rows;
    }

    public <T> T selectRow(final Class<T> clazz) {
        Map<String, Object> row = selectRow();
        return row.isEmpty() ? null : objectMapper.convertValue(row, clazz);
    }

    static void setParams(final PreparedStatement pstmt, final List<Object> sqlParams) throws SQLException {
        for (int i = 0; i < sqlParams.size(); i++) {
            Object param = sqlParams.get(i);
            if (param instanceof String)
                pstmt.setString(i + 1, (String) param);
            else if (param instanceof Integer)
                pstmt.setInt(i + 1, (Integer) param);
            else if (param instanceof Boolean)
                pstmt.setBoolean(i + 1, (Boolean) param);
            else if (param instanceof Double)
                pstmt.setDouble(i + 1, (Double) param);
            else if (param instanceof Long)
                pstmt.setLong(i + 1, (Long) param);
            else if (param instanceof LocalDateTime)
                pstmt.setDate(i + 1, Date.valueOf(((LocalDateTime) param).toLocalDate()));
        }
    }

}
