package baekgwa.com.ll.simpleDb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Setter;

/**
 * 역할)
 * 0. DB 연결
 * 1. Connection 관리
 * 2. 트랜잭션 관리
 *
 * Connection pull default : Lazy Initialization
 * `Eager Initialization` is not completed
 */
public class SimpleDb {

    //docker
    //docker run --name simpleDbTest -e MYSQL_ROOT_PASSWORD=1234 -d -p 3334:3306 mysql:8.0

    //db info
    private final String url;
    private final String username;
    private final String password;

    //connection poll
    private final Queue<Connection> pool = new ConcurrentLinkedQueue<>();
    private final int maxConnections; //최대 연결 수
    private final int DEFAULT_MAX_CONNECTIONS = 10;
    private final AtomicInteger currentConnections = new AtomicInteger(0);

    /*
        -dev mode-
        true : 테스트 에서 로그 출력
        false : 운영환경, 로그 미출력
    */
    @Setter
    private boolean devMode = false;

    /**
     * sql 문을 실행하고 자동으로 Connection을 반납하는 기능
     * 트랜잭션 범위 내에서는 false 로 설정
     */
    @Setter
    private boolean autoCloseConnection = true;

    /**
     * Only MySQL DB
     */
    public SimpleDb(String host, String username, String password, String dbName, String port) {
        this.password = password;
        this.username = username;
        this.url = String.format("jdbc:mysql://%s:%s/%s", host, port, dbName);

        maxConnections = DEFAULT_MAX_CONNECTIONS;
    }

    /**
     * JDBC url 로 연결
     */
    public SimpleDb(String url, String username, String password, int maxConnections) {
        this.password = password;
        this.username = username;
        this.url = url;
        this.maxConnections = maxConnections;
    }

    /**
     * 커넥션 풀에서, 커넥션을 전달해줍니다.
     * 커넥션 풀에 없으면, 새로 만들어서 전달합니다.
     * 최대로 설정된 maxPoolSize 보다 많이 생성될 수 없으며, 생성을 요청하면 exception 발생
     * (--음.. 대기 기능은 만들지 말지 고민중 귀찮음--)
     * @return
     * @throws SQLException
     */
    private Connection getConnection() throws SQLException {
        if(!pool.isEmpty()) {
            return pool.poll();
        } else if(currentConnections.get() < maxConnections) {
            currentConnections.incrementAndGet();
            return newConnection();
        } else {
            throw new SQLException("Now connection is Max, maxConnections:" + this.maxConnections);
        }
    }

    private Connection newConnection() throws SQLException {
        return DriverManager.getConnection(this.url, this.username, this.password);
    }

    public void returnConnection(Connection connection) {
        if(connection != null) {
            pool.offer(connection);
        }
    }

    public Connection run(String sql) {
        Sql newSql = genSql();
        Connection connection = newSql.getConnection();

        newSql.execute(sql);
        returnConnection(connection);
        return connection;
    }

    public Connection run(String sql, Object...params) {
        Sql newSql = genSql();
        Connection connection = newSql.getConnection();

        newSql.execute(sql, params);
        returnConnection(connection);
        return connection;
    }

    public Sql genSql() {
        Connection connection = null;
        try {
            connection = getConnection();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return new Sql(connection, this, devMode, autoCloseConnection); //ㄹㅇ 쓰레기 코드. 마음에안듬
    }

    public void startTransaction(Connection connection) {
        try {
            connection.setAutoCommit(false);
            setAutoCloseConnection(false);
        } catch (SQLException e) {
            setAutoCloseConnection(true);
            throw new RuntimeException(e);
        }
    }

    public void rollback(Connection connection) {
        try {
            connection.rollback();
            connection.setAutoCommit(true);
            setAutoCloseConnection(true);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            setAutoCloseConnection(true);
        }
    }

    public void commit(Connection connection) {
        try {
            connection = getConnection();
            connection.commit();
            connection.setAutoCommit(true);
            setAutoCloseConnection(true);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            setAutoCloseConnection(true);
        }
    }

    public void closeConnection() {
    }
}
