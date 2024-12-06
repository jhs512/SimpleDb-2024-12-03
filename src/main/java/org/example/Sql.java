package org.example;

import java.time.LocalDateTime;
import java.util.*;

public class Sql {
    private final StringBuilder sql = new StringBuilder();
    private final SimpleDb simpleDb;
    private final List<Object> params = new ArrayList<>();

    public Sql(SimpleDb simpleDb) {
        this.simpleDb = simpleDb;
    }

    public Sql append(String sql, Object... params) {
        this.sql.append(sql).append("\n");
        this.params.addAll(Arrays.asList(params));
        return this;
    }

    public Sql appendIn(String sql, Object... params) {
        StringBuilder parsedParam = new StringBuilder();

        // MySQL은 PreparedStatement.setArray 메서드를 지원하지 않기 때문에 직접 문자열로 파싱
        for (int i = 0; i < params.length; i++) {
            parsedParam.append("'").append(params[i]).append("'");
            if (i != params.length - 1) parsedParam.append(",");
        }
        sql = sql.replaceFirst("\\?", parsedParam.toString());

        this.sql.append(sql).append("\n");
        return this;
    }

    public long insert() {
        simpleDb.setSqlAndParams(sql.toString().trim(), params.toArray());
        return simpleDb.insert();
    }

    public int update() {
        simpleDb.setSqlAndParams(sql.toString(), params.toArray());
        return simpleDb.update();
    }

    public int delete() {
        simpleDb.setSqlAndParams(sql.toString(), params.toArray());
        return simpleDb.delete();
    }

    public List<Map<String, Object>> selectRows() {
        simpleDb.setSqlAndParams(sql.toString(), params.toArray());
        return simpleDb.selectRows();
    }

    public List<Article> selectRows(Class<Article> articleClass) {
        simpleDb.setSqlAndParams(sql.toString(), params.toArray());
        return simpleDb.selectRows(articleClass);
    }

    public Map<String, Object> selectRow() {
        simpleDb.setSqlAndParams(sql.toString(), params.toArray());
        return selectRows().getFirst();
    }

    public Article selectRow(Class<Article> articleClass) {
        simpleDb.setSqlAndParams(sql.toString(), params.toArray());
        return selectRows(Article.class).getFirst();
    }

    public LocalDateTime selectDatetime() {
        simpleDb.setSqlAndParams(sql.toString(), params.toArray());
        return simpleDb.selectDatetime();
    }

    public Long selectLong() {
        simpleDb.setSqlAndParams(sql.toString(), params.toArray());
        return simpleDb.selectLong();
    }

    public List<Long> selectLongs() {
        simpleDb.setSqlAndParams(sql.toString(), params.toArray());
        return simpleDb.selectLongs();
    }

    public String selectString() {
        simpleDb.setSqlAndParams(sql.toString(), params.toArray());
        return simpleDb.selectString();
    }

    public Boolean selectBoolean() {
        simpleDb.setSqlAndParams(sql.toString(), params.toArray());
        return simpleDb.selectBoolean();
    }

}