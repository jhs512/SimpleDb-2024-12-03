package com.ll.simpleDb.Sql;

import com.ll.Article.Article;

import java.sql.Connection;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

public class SqlDevImpl extends SqlImpl {

    public SqlDevImpl(Connection connection) {
        super(connection);
    }

    public long insert(){
        System.out.println(super.sql);
        return super.insert();
    }

    public long update(){
        System.out.println(super.sql);
        return super.update();
    }

    public long delete(){
        System.out.println(super.sql);
        return super.delete();
    }

    public List<Map<String, Object>> selectRows() {
        System.out.println(super.sql);
        return super.selectRows();
    }

    public List<Article> selectRows(Class<Article> a) {
        System.out.println(super.sql);
        return super.selectRows(Article.class);
    }

    public Map<String, Object> selectRow() {
        System.out.println(super.sql);
        return super.selectRow();
    }

    public Article selectRow(Class<Article> a) {
        System.out.println(super.sql);
        return super.selectRow(Article.class);
    }

    public LocalDateTime selectDateTime() {
        System.out.println(super.sql);
        return super.selectDatetime();
    }

    public long selectLong() {
        System.out.println(super.sql);
        return super.selectLong();
    }

    public String selectString() {
        System.out.println(super.sql);
        return super.selectString();
    }

    public Boolean selectBoolean() {
        System.out.println(super.sql);
        return super.selectBoolean();
    }

    public List<Long> selectLongs() {
        System.out.println(super.sql);
        return super.selectLongs();
    }

}

