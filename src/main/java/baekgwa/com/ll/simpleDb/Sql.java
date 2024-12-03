package baekgwa.com.ll.simpleDb;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * 역할)
 * 1. Connection 사용해서 실제 SQL 실행 2.
 * 2. devMode 에 따른 출력 처리
 * 3. 최대한 공통으로 사용할 수 있도록 처리.
 */
@RequiredArgsConstructor
public class Sql implements SqlDefine {

    @Getter
    private final Connection connection; //해당 SQL을 실행하기 위해 할당된 Connection
    private final StringBuffer sqlBuilder = new StringBuffer(); //실행할 sql문을 담아두는 buffer 역할
    private final SimpleDb simpleDb; //connection 관리자
    private final boolean devMode; //sql 출력 기능 on/off
    private final boolean autoCloseConnection; //autoCloseConnection 기능 on/off

    @Override
    public void execute(String inputSql) {
        try {
            Statement statement = connection.createStatement();
            statement.execute(inputSql);
            printRowSql(inputSql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            if(autoCloseConnection) {
                simpleDb.returnConnection(connection);
            }
        }
    }

    @Override
    public void execute(String inputSql, Object... params) {
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(inputSql);
            for (int i = 0; i < params.length; i++) {
                preparedStatement.setObject(i + 1, params[i]);
            }
            preparedStatement.execute();
            printRowSql(preparedStatement.toString());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            if(autoCloseConnection) {
                simpleDb.returnConnection(connection);
            }
        }
    }

    @Override
    public long insert() {
        try {
            Statement statement = connection.createStatement();
            statement.execute(sqlBuilder.toString(), Statement.RETURN_GENERATED_KEYS);
            printRowSql(sqlBuilder.toString());

            ResultSet generatedKeys = statement.getGeneratedKeys();
            generatedKeys.next();
            return generatedKeys.getLong(1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            if(autoCloseConnection) {
                simpleDb.returnConnection(connection);
            }
        }
    }

    @Override
    public long update() {
        try {
            Statement statement = connection.createStatement();
            printRowSql(sqlBuilder.toString());
            return statement.executeUpdate(sqlBuilder.toString());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            if(autoCloseConnection) {
                simpleDb.returnConnection(connection);
            }
        }
    }

    @Override
    public long delete() {
        try {
            Statement statement = connection.createStatement();
            printRowSql(sqlBuilder.toString());
            return statement.executeUpdate(sqlBuilder.toString());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            if(autoCloseConnection) {
                simpleDb.returnConnection(connection);
            }
        }
    }

    @Override
    public List<Map<String, Object>> selectRows() {
        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sqlBuilder.toString());
            printRowSql(sqlBuilder.toString());

            ResultSetMetaData metaData = resultSet.getMetaData();
            List<Map<String, Object>> result = new ArrayList<>();
            while (resultSet.next()) {
                Map<String, Object> row = new HashMap<>();
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    row.put(
                            metaData.getColumnName(i),
                            resultSet.getObject(i)
                    );
                }
                result.add(row);
            }
            return result;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            if(autoCloseConnection) {
                simpleDb.returnConnection(connection);
            }
        }
    }

    @Override
    public <T> List<T> selectRows(Class<T> tClass) {
        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sqlBuilder.toString());
            printRowSql(sqlBuilder.toString());
            List<T> resultList = new ArrayList<>();

            while (resultSet.next()) {
                T instance = tClass.getDeclaredConstructor().newInstance();

                for (Field field : tClass.getDeclaredFields()) {
                    field.setAccessible(true);
                    String columnName = field.getName();
                    Object columnValue = resultSet.getObject(columnName);

                    // 필드에 값을 설정
                    field.set(instance, columnValue);
                }
                resultList.add(instance);
            }

            return resultList;
        } catch (SQLException | ReflectiveOperationException e) {
            throw new RuntimeException(e);
        } finally {
            if(autoCloseConnection) {
                simpleDb.returnConnection(connection);
            }
        }
    }

    @Override
    public Map<String, Object> selectRow() {
        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sqlBuilder.toString());
            printRowSql(sqlBuilder.toString());

            ResultSetMetaData metaData = resultSet.getMetaData();
            Map<String, Object> result = new HashMap<>();
            resultSet.next();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                result.put(
                        metaData.getColumnName(i),
                        resultSet.getObject(i)
                );
            }
            return result;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            if(autoCloseConnection) {
                simpleDb.returnConnection(connection);
            }
        }
    }

    @Override
    public <T> T selectRow(Class<T> tClass) {
        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sqlBuilder.toString());
            printRowSql(sqlBuilder.toString());

            resultSet.next();
            T instance = tClass.getDeclaredConstructor().newInstance();

            for (Field field : tClass.getDeclaredFields()) {
                field.setAccessible(true);
                String columnName = field.getName();
                Object columnValue = resultSet.getObject(columnName);

                // 필드에 값을 설정
                field.set(instance, columnValue);
            }
            return instance;
        } catch (SQLException | ReflectiveOperationException e) {
            throw new RuntimeException(e);
        } finally {
            if(autoCloseConnection) {
                simpleDb.returnConnection(connection);
            }
        }
    }

    @Override
    public LocalDateTime selectDatetime() {
        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sqlBuilder.toString());
            printRowSql(sqlBuilder.toString());

            resultSet.next();
            return resultSet.getTimestamp(1).toLocalDateTime();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            if(autoCloseConnection) {
                simpleDb.returnConnection(connection);
            }
        }
    }

    @Override
    public long selectLong() {
        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sqlBuilder.toString());
            printRowSql(sqlBuilder.toString());

            resultSet.next();
            return resultSet.getLong(1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            if(autoCloseConnection) {
                simpleDb.returnConnection(connection);
            }
        }
    }

    @Override
    public String selectString() {
        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sqlBuilder.toString());
            printRowSql(sqlBuilder.toString());

            resultSet.next();
            return resultSet.getString(1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            if(autoCloseConnection) {
                simpleDb.returnConnection(connection);
            }
        }
    }

    @Override
    public boolean selectBoolean() {
        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sqlBuilder.toString());
            printRowSql(sqlBuilder.toString());

            resultSet.next();
            return resultSet.getBoolean(1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            if(autoCloseConnection) {
                simpleDb.returnConnection(connection);
            }
        }
    }

    @Override
    public List<Long> selectLongs() {
        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sqlBuilder.toString());
            printRowSql(sqlBuilder.toString());

            ArrayList<Long> result = new ArrayList<>();
            while (resultSet.next()) {
                result.add(resultSet.getLong(1));
            }
            return result;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            if(autoCloseConnection) {
                simpleDb.returnConnection(connection);
            }
        }
    }

    @Override
    public Sql append(String inputSql) {
        sqlBuilder.append(inputSql + " ");
        return this;
    }

    @Override
    public Sql append(String inputSql, Object... params) {
        for (Object param : params) {
            inputSql = inputSql.replaceFirst("\\?", "'" + param + "'");
        }
        sqlBuilder.append(inputSql);

        return this;
    }

    @Override
    public Sql appendIn(String inputSql, Object... params) {
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < params.length; i++) {
            stringBuilder.append("'").append(params[i]).append("'");
            if (i < params.length - 1) {
                stringBuilder.append(", ");
            }
        }
        inputSql = inputSql.replaceFirst("\\?", stringBuilder.toString());
        this.sqlBuilder.append(inputSql);
        return this;
    }

    @Override
    public void printRowSql(String inputSql) {
        if (devMode) {
            System.out.println("== rawSql ==");
            System.out.println(inputSql);
            System.out.println();
        }
    }
}
