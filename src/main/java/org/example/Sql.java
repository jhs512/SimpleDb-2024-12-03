package org.example;

import java.sql.*;

public class Sql {
    private StringBuilder sql = new StringBuilder();
    private SimpleDb simpleDb;
    private final Connection connection;
    private PreparedStatement preparedStatement;

    public Sql(SimpleDb db) {
        simpleDb = db;
        this.connection = db.getConnection();
    }
    public Sql append(String sql) {
        this.sql.append(sql).append("\n");
        return this;
    }
    public Sql append(String sql, String param) {
        sql = sql.replace("?","\"" + param + "\"");
        this.sql.append(sql).append("\n");
        return this;
    }

    public Sql append(String sql, int...params) {
        for (int param : params) {
            sql = sql.replaceFirst("\\?", param+"");
            System.out.println("sql = " + sql);
        }
        this.sql.append(sql).append("\n");
        return this;
    }



    public long insert() {
        long id = -1;
        try{
            preparedStatement = connection.prepareStatement(sql.toString(), Statement.RETURN_GENERATED_KEYS);
            preparedStatement.executeUpdate();
            ResultSet generatedKey = preparedStatement.getGeneratedKeys();
            if(generatedKey.next()) { id = generatedKey.getLong(1); }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        showSql();
        return id;
    }

    public long update() {
        long affectedRows = excuteUpdateSql();
        showSql();
        return affectedRows;
    }

    public long delete() {
        long affectedRows = excuteUpdateSql();
        showSql();
        return affectedRows;
    }

    private long excuteUpdateSql() {
        long affectedRows = 0;
        try {
            preparedStatement = connection.prepareStatement(sql.toString());
            affectedRows = preparedStatement.executeUpdate();
        } catch (SQLException e) {
            System.out.println("sql 입력 오류 = " + e);
        }
        return affectedRows;
    }


    private void showSql() {
        if(simpleDb.getDevMode()) {
            System.out.println("== rawSql ==");
            System.out.println("sql = " + sql);
        }
    }
}
