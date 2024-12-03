package com.ll;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public interface Sql {

    SqlImpl append(String queryPiece, Object... values) ;
    long update();
    long delete() ;
    long insert() ;

    List<Map<String, Object>> selectRows() ;
    Map<String, Object> selectRow();

    LocalDateTime selectDatetime();
    Long selectLong();
    String selectString();
    Boolean selectBoolean();
}
