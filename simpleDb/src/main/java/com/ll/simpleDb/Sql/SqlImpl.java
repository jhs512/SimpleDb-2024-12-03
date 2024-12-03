package com.ll.simpleDb.Sql;

import com.ll.Article.Article;
import com.ll.simpleDb.SimpleDbImpl;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;

public class SqlImpl implements Sql {

    protected StringBuilder sql;
    private Connection conn;
    private PreparedStatement pstmt;
    private List<Object> SqlVariables = new ArrayList<>();
    public final UUID id = UUID.randomUUID();

    public SqlImpl(Connection connection) {
        this.sql = new StringBuilder();
        this.conn = connection;
    }

    private void setQuery() throws SQLException {
        pstmt = conn.prepareStatement(this.sql.toString(), Statement.RETURN_GENERATED_KEYS);
        for (int i = 0; i < SqlVariables.size(); i++) {
            if (SqlVariables.get(i) != "") pstmt.setObject(i+1, SqlVariables.get(i));
        }
    }

    @Override
    public SqlImpl append(String sql) {
       return this.append(sql, "");
    }

    public SqlImpl append(String sql, Object...params) {
        this.sql.append(sql);
        this.sql.append("\n");
        for (Object param : params) {
            if (param != "") this.SqlVariables.add(param);
        }

        return this;
    }

    @Override
    public SqlImpl appendIn(String sql, Object...params) {
        String beforeSql = sql.split("\\?")[0];
        String afterSql = sql.split("\\?")[1];

        StringBuilder additional = new StringBuilder();
        additional.append(beforeSql);
        additional.append("?,".repeat(Arrays.stream(params).toList().size()));
        additional.deleteCharAt(additional.length()-1);
        additional.append(afterSql);

        this.sql.append(additional);
        this.sql.append("\n");
        this.SqlVariables.addAll(Arrays.asList(params));
        return this;
    }

    @Override
    public long insert() {
        long id = -1;

        try {
            setQuery();
            pstmt.executeUpdate();
            ResultSet generatedKeys = pstmt.getGeneratedKeys();
            if (generatedKeys.next()) {
                id = generatedKeys.getInt(1);
            }
            pstmt.close();
            return id;
        } catch (SQLException e) {
            //쿼리에 실패했을 때 -1 반환
            return id;
        }
    }

    @Override
    public long update() {
        try {
            setQuery();
            return pstmt.executeUpdate();

        } catch (SQLException e) {
            //쿼리에 실패했을 때 -1 반환
            return -1;
        }
    }

    @Override
    public long delete() {
        /*
        코드가 update 메서드와 중복되어 update 메서드를 호출하였습니다
        추후에 로직 변경 시 확인바랍니다
        */
        return update();
    }

    @Override
    public List<Map<String, Object>> selectRows() {
        List<Map<String, Object>> result = new ArrayList<>();
        try {
            setQuery();
            ResultSet rs = pstmt.executeQuery();
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnName(i);
                    Object columnValue = rs.getObject(i);
                    row.put(columnName, columnValue);
                }
                result.add(row);
            }
            return result;

        } catch (SQLException e) {
            //쿼리에 실패했을 때 빈 리스트 반횐
            return result;
        }
    }

    public List<Article> selectRows(Class<Article> article) {
        List<Article> result = new ArrayList<>();
        try {
            setQuery();
            ResultSet rs = pstmt.executeQuery();
            ResultSetMetaData metaData = rs.getMetaData();

            while (rs.next()) {
                Article row = new Article();
                row.setId(rs.getLong(1));
                row.setCreatedDate(rs.getTimestamp(2).toLocalDateTime());
                row.setModifiedDate(rs.getTimestamp(3).toLocalDateTime());
                row.setTitle(rs.getString(4));
                row.setBody(rs.getString(5));
                row.setIsBlind(rs.getBoolean(6));

                result.add(row);
            }
            return result;

        } catch (SQLException e) {
            //쿼리에 실패했을 때 빈 리스트 반횐
            return result;
        }
    }

    @Override
    public Map<String, Object> selectRow() {
        /*
        코드가 selectRows 메서드와 중복되어 selectRows 메서드를 호출하였습니다
        추후에 로직 변경 시 확인바랍니다
        */
        return selectRows().getFirst();
    }

    public Article selectRow(Class<Article> article) {
        /*
        코드가 selectRows 메서드와 중복되어 selectRows 메서드를 호출하였습니다
        추후에 로직 변경 시 확인바랍니다
        */
        return selectRows(Article.class).getFirst();
    }

    @Override
    public LocalDateTime selectDatetime() {
        try {
            setQuery();
            ResultSet rs = pstmt.executeQuery();

            if (rs.next()) {
                Timestamp currentTime = rs.getTimestamp(1);
                return currentTime.toLocalDateTime();
            }
        } catch (SQLException e) {
            //쿼리에 실패했을 때 null 반환
            return null;
        }
        return null;
    }

    @Override
    public long selectLong() {
        //long 타입 컬럼의 첫 번째 행을 반환합니다
        try {
            setQuery();
            ResultSet rs = pstmt.executeQuery();

            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            while (rs.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    return rs.getLong(i);
                }
            }

        } catch (SQLException e) {
            //쿼리에 실패했을 때 -1 반환
            return -1;
        }
        return -1;
    }

    @Override
    public String selectString() {
        //String 타입 컬럼의 첫 번째 행을 반환합니다
        try {
            setQuery();
            ResultSet rs = pstmt.executeQuery();

            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            while (rs.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    return rs.getString(i);
                }
            }

        } catch (SQLException e) {
            //쿼리에 실패했을 때 -1 반환
            return "";
        }
        return "";
    }

    @Override
    public Boolean selectBoolean() {
        try {
            setQuery();
            ResultSet rs = pstmt.executeQuery();

            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            while (rs.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    return rs.getBoolean(i);
                }
            }

        } catch (SQLException e) {
            //쿼리에 실패했을 때 -1 반환
            return null;
        }
        return null;
    }

    @Override
    public List<Long> selectLongs() {
        List<Long> result = new ArrayList<>();
        try {
            setQuery();
            ResultSet rs = pstmt.executeQuery();

            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            while (rs.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    result.add(rs.getLong(i));
                }
            }

        } catch (SQLException e) {
            //쿼리에 실패했을 때 -1 반환
            return result;
        }
        return result;
    }
}
