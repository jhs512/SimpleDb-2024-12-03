package com.ll.simpleDb;

import lombok.SneakyThrows;

import java.lang.reflect.Field;
import java.time.LocalDateTime;
import java.util.*;

public class Sql {
    private final SimpleDb simpleDb;
    private final StringBuilder query;
    private final List<Object> params;

    public Sql(SimpleDb simpleDb) {
        this.simpleDb = simpleDb;
        this.query = new StringBuilder();
        this.params = new ArrayList<>();
    }

    public Sql append(String query, Object... params) {
        this.query.append(" ").append(query);
        if (params != null) {
            this.params.addAll(Arrays.asList(params));
        }
        return this;
    }

    public Sql appendIn(String query, Object... params) {
        if (params != null) {
            // params 개수만큼 생성
            String placeHolder = String.join(",", Collections.nCopies(params.length, "?"));

            int idx = query.indexOf('?');
            if (idx != -1) {
                String updateQuery = query.substring(0, idx) + placeHolder + query.substring(idx + 1);
                this.query.append(" ").append(updateQuery);
                this.params.addAll(Arrays.asList(params));
            }
        }
        return this;
    }

    public long insert() {
        return (long) commonSql("INSERT");
    }

    public int update() {
        return (int) commonSql("UPDATE");
    }

    public int delete() {
        return (int) commonSql("DELETE");
    }

    @SuppressWarnings("unchecked")
    public List<Map<String, Object>> selectRows() {
        return (List<Map<String, Object>>) commonSql("SELECT");
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> selectRow() {
        List<Map<String, Object>> list = (List<Map<String, Object>>) commonSql("SELECT");
        return list.get(0);
    }

    public LocalDateTime selectDatetime() {
        Map<String, Object> map = selectRow();

        return (LocalDateTime) map.get("NOW()");
    }

    public Long selectLong() {
        Map<String, Object> map = selectRow();
        String key = map.keySet().iterator().next();

        return (Long) map.get(key);
    }

    public String selectString() {
        Map<String, Object> map = selectRow();
        return (String) map.get("title");
    }

    public Boolean selectBoolean() {
        Map<String, Object> map = selectRow();

        String key = map.keySet().iterator().next();

        Object value = map.get(key);

        if (value instanceof Boolean) return (Boolean) value;
        else if (value instanceof Number) return ((Number) value).intValue() == 1;

        return (Boolean) map.get(key);
    }

    public List<Long> selectLongs() {
        List<Long> result = new ArrayList<>();

        List<Map<String, Object>> list = selectRows();
        for (Map<String, Object> map : list) {
            for (String key : map.keySet()) {
                result.add((Long) map.get(key));
            }
        }

        return result;
    }

    public <T> List<T> selectRows(Class<?> cls) {
        return selectRows()
                .stream()
                .map(row -> (T) mapToClass(row, cls))
                .toList();
    }

    public <T> T selectRow(Class<?> cls) {
        return (T) mapToClass(selectRow(), cls);
    }

    private Object commonSql(String command) {
        return simpleDb.dbCommand(command, query.toString(), params.toArray());
    }

    @SneakyThrows
    private <T> T mapToClass(Map<String, Object> row, Class<T> cls) {
        T obj = cls.getDeclaredConstructor().newInstance();

        for (Field field : cls.getDeclaredFields()) {
            field.setAccessible(true);
            Object value = row.get(field.getName());
            if (value != null) {
                field.set(obj, value);
            }
        }
        return obj;
    }
}
