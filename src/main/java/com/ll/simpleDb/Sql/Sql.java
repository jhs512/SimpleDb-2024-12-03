package com.ll.simpleDb.Sql;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

/**
 * @apiNote 실제 데이터베이스의 쿼리를 실행 및 관리합니다
 */
public interface Sql {
    /**
     *
     * @param sql
     * @return SqlImpl
     * @apiNote 내부적으로 StringBuilder를 이용하여 sql 쿼리를 append합니다
     */
    SqlImpl append(String sql);
    /**
     *
     * @param sql
     * @return SqlImpl
     * @apiNote 내부적으로 StringBuilder를 이용하여 sql 쿼리를 append합니다, 반드시 가변 인자를 받아야 합니다
     */
    SqlImpl appendIn(String sql, Object...params);
    long insert();
    long update();
    long delete();
    List<Map<String, Object>> selectRows();
    Map<String, Object> selectRow();
    LocalDateTime selectDatetime();
    long selectLong();
    String selectString();
    Boolean selectBoolean();
    List<Long> selectLongs();
}
