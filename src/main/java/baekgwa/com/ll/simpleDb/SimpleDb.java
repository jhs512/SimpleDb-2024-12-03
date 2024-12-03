package baekgwa.com.ll.simpleDb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import lombok.Setter;

public class SimpleDb {

    //docker
    //docker run --name simpleDbTest -e MYSQL_ROOT_PASSWORD=1234 -d -p 3334:3306 mysql:8.0

    //db info
    private final String url;
    private final String host;
    private final String username;
    private final String password;
    private final String dbName;
    private final String port = "3334";

    //connection poll
    private final Queue<Connection> pool;
    private final int maxConnections = 10; //최대 연결 수
    private int currentConnections = 0;

    /*
        -dev mode-
        true : 테스트 에서 로그 출력
        false : 운영환경, 로그 미출력
    */
    @Setter
    private boolean devMode = false;

    public SimpleDb(String host, String username, String password, String dbName) {
        this.dbName = dbName;
        this.password = password;
        this.username = username;
        this.host = host;
        this.url = String.format("jdbc:mysql://%s:%s/%s", this.host, this.port, this.dbName); //정말 싫지만, 테스트코드가 고정이라...
        pool = new ConcurrentLinkedQueue<>();
    }

    private Connection getConnection() throws SQLException {
        if(!pool.isEmpty()) {
            return pool.poll();
        } else if(currentConnections < maxConnections) {
            currentConnections++;
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

    private void terminateAllConnections() throws SQLException {
        for (Connection connection : pool) {
            connection.close();
        }
        pool.clear();
        currentConnections = 0;
    }

    public void run(String sql) {
        Connection connection = null;
        try {
            connection = getConnection();
            Statement statement = connection.createStatement();
            statement.execute(sql);
            printLog(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            if(connection != null) {
                returnConnection(connection);
            }
        }
    }

    public void run(String sql, Object...params) {
        Connection connection = null;
        try {
            connection = getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            for(int i=0; i<params.length; i++) {
                preparedStatement.setObject(i + 1, params[i]);
            }
            preparedStatement.execute();
            printLog(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            if(connection != null) {
                returnConnection(connection);
            }
        }
    }

    private void printLog(String sql) {
        if (devMode) {
            System.out.println("== rawSql ==");
            System.out.println(sql);
            System.out.println();
        }
    }

    public Sql genSql() {
        Connection connection = null;
        try {
            connection = getConnection();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return new Sql(connection, this);
    }

    public void startTransaction() {
        Connection connection = null;
        try {
            connection = getConnection();
            connection.setAutoCommit(false);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            if(connection != null) {
                returnConnection(connection);
            }
        }
    }

    public void rollback() {
        Connection connection = null;
        try {
            connection = getConnection();
            connection.rollback();
            connection.setAutoCommit(true);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            if(connection != null) {
                returnConnection(connection);
            }
        }
    }

    public void commit() {
        Connection connection = null;
        try {
            connection = getConnection();
            connection.commit();
            connection.setAutoCommit(true);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void closeConnection() {
        //@Deprecated
        //난 쓸필요가 없긴한데,,, 강사님 예제에 맞추다 보니 생겨버린 메서드 ㅜ
    }
}
