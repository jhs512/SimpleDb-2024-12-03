package com.ll.simpleDb;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class Sql {
    private final SimpleDb simpleDb;
    private final StringBuilder sql;
    private final List<Object> param;

    public Sql(SimpleDb simpleDb){
        this.simpleDb = simpleDb;
        this.sql = new StringBuilder();
        this.param = new ArrayList<>();
    }

    public Sql append(Object... params) {
        for(int i=0; i<params.length; i++){
            if(i>0){
                param.add(String.valueOf(params[i]));
            }else{
                sql.append(params[i]).append(" ");
            }
        }
        return this;
    }

    public Sql appendIn(String s, Object... params) {
        String placeholders = String.join(",", Collections.nCopies(params.length, "?"));
        sql.append(s.replace("?", placeholders));

        for (Object o : params) {
            param.add(String.valueOf(o));
        }
        return this;
    }

    private String toSql(){
        return sql.toString();
    }

    public long insert() {
        return simpleDb.insert(toSql(), param.toArray());
    }

    public int update() {
        return simpleDb.update(toSql(), param.toArray());

    }

    public int delete() {
        return simpleDb.delete(toSql(), param.toArray());
    }

    public List<Map<String, Object>> selectRows() {
        return simpleDb.selectRows(toSql(), param.toArray());
    }

    public <T> List<T> selectRows(Class<?> cls) {
        return simpleDb.selectRows(toSql(), cls, param.toArray());
    }

    public Map<String, Object> selectRow() {
        return simpleDb.selectRow(toSql(), param.toArray());
    }

    public <T> T  selectRow(Class<?> cls) {
        return simpleDb.selectRow(toSql(), cls, param.toArray());
    }

    public LocalDateTime selectDatetime() {
        return simpleDb.selectDatetime(toSql(), param.toArray());
    }

    public Long selectLong() {
        return simpleDb.selectLong(toSql(), param.toArray());
    }

    public List<Long> selectLongs() {
        return simpleDb.selectLongs(toSql(), param.toArray());
    }

    public String selectString() {
        return simpleDb.selectString(toSql(), param.toArray());
    }

    public Boolean selectBoolean() {
        return simpleDb.selectBoolean(toSql(), param.toArray());
    }
}
