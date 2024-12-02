package com.ll.simpleDb;

import com.ll.simpleDb.Sql.SqlImpl;

/**
 * @apiNote 데이터베이스의 연결, 트랜잭션 관리를 관리합니다
 */

public interface SimpleDb {
    /**
    * @param isDevMode
     * @apiNote
     * * 터미널에 실행한 쿼리와 결과를 출력하게끔 설정합니다
    */
    void setDevMode(Boolean isDevMode);

    /**
     * @param sql
     * @return boolean, 내부적으로 Sql객체를 생성 후 update 메서드를 실행합니다
     * @apiNote 임의로 sql쿼리를 실행합니다. 트랜잭션, 롤백, 멀티 스레드는 지원되지 않습니다.
     * 해당 기능을 사용하기 위해서는 SqlImpl 객체의 메서드를 사용부탁드립니다
     */
    long run(String sql);
    /**
     *
     * @return SqlImpl SqlImpl 객체를 리턴합니다
     * @apiNote 데이터베이스와 실질적으로 통신합니다 genSql은 내부적으로 Connection을 닫지 않습니다.
     * genSql 호출 후에는 반드시 closeConnection 메서드를 이용하여 자원을 닫아주십시오
     * 메서드를 한 번 호출할 때마다 새로운 커넥션을 연결하기 때문에 멀티스레드 환경에서도 이용이 가능합니다
     */
    SqlImpl genSql();
    /**
     * @apiNote SqlImpl id를 인자로 받아 커넥션을 종료합니다
     */
    void closeConnection(int id);
    /**
     * @apiNote 트랜잭션을 시작합니다 내부적으로 JDBC의 오토 커밋을 false로 설정합니다
     */
    void startTransaction(int id);
    /**
     * @apiNote 데이터베이스를 롤백합니다 내부적으로 rollback() 함수를 이용합니다
     */
    void rollback(int id);
    /**
     * @apiNote 데이터베이스를 커밋합니다
     */
    void commit(int id);

}
