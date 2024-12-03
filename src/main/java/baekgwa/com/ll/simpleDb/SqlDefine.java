package baekgwa.com.ll.simpleDb;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

public interface SqlDefine {
    long insert(); //return 생성된 id
    long update(); //return 업데이트 된 row 개수
    long delete(); //return 삭제된 row 개수

    List<Map<String, Object>> selectRows(); //key,value 데이터 리스트 return
    <T> List<T> selectRows(Class<T> tClass);

    Map<String, Object> selectRow(); //key, value 형으로 데이터 1개 return
    <T> T selectRow(Class<T> tClass);

    LocalDateTime selectDatetime();
    long selectLong();
    String selectString();
    boolean selectBoolean();
    List<Long> selectLongs();
    
    Sql append(String sql);
    Sql append(String sql, Object...params);
    Sql appendIn(String s, Object...params);
}
