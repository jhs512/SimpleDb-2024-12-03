package com.ll;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Sql {

    public Sql(SimpleDb simpleDb) {
        this.simpleDb = simpleDb;
    }

    private final SimpleDb simpleDb;
    StringBuilder query = new StringBuilder();
    List<Object> params = new ArrayList<>();

    public Sql append(String queryPiece, Object... values) {
        query.append(queryPiece);
        query.append(" ");

        if(queryPiece.replaceAll("[^?]", "").length() == values.length) {
            params.addAll(Arrays.asList(values));
        }
        return this;
    }

    public long insert() {
        return simpleDb.insert(query.toString(), params);
    }

    public long update() {
        return simpleDb.update(query.toString(), params);
    }

    public long delete() {
        return simpleDb.delete(query.toString(), params);
    }
}
