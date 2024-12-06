package com.ll;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

public interface Sql extends AutoCloseable {

    SqlImpl append(String queryPiece, Object... values) ;
    SqlImpl appendIn(String s, Object... values);

    long update();
    long delete() ;
    long insert() ;

    List<Map<String, Object>> selectRows() ;
    <T> List<T> selectRows(Class<T> clazz);
    Map<String, Object> selectRow();
    <T> T selectRow(Class<T> clazz);

    LocalDateTime selectDatetime();
    Long selectLong();
    String selectString();
    Boolean selectBoolean();

    List<Long> selectLongs();

    void close();

    public Connection getConnection();
}
