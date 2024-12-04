package org.example;

import java.time.LocalDateTime;
import java.util.*;

public class Sql {
    private String query = "";
    private List<Object> params = new ArrayList<>();
    private SimpleDb db;

    public Sql(SimpleDb db) {
        this.db = db;
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

    public long insert() {
        return db.runAndGetGeneratedKey(query, getParamsArray());
    }

    public long update() {
        return db.runAndGetAffectedRowsCount(query, getParamsArray());
    }

    public long delete() {
        return db.runAndGetAffectedRowsCount(query, getParamsArray());
    }

    public List<Map<String, Object>> selectRows() {
        return db.selectRows(query, getParamsArray());
    }

    public Map<String, Object> selectRow() {
        return db.selectRow(query, getParamsArray());
    }

    public LocalDateTime selectDatetime() {
        return db.selectDatetime(query, getParamsArray());
    }

    public Long selectLong() {
        return db.selectLong(query, getParamsArray());
    }

    public String selectString() {
        return db.selectString(query, getParamsArray());
    }

    public Boolean selectBoolean() {
        return db.selectBoolean(query, getParamsArray());
    }

    private Object[] getParamsArray() {
        return params.toArray();
    }
}
