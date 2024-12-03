package baekgwa.com.ll.simpleDb;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class Sql implements SqlDefine {

    private final Connection connection;
    private StringBuffer sb = new StringBuffer();
    private final SimpleDb simpleDb;

    @Override
    public long insert() {
        try {
            Statement statement = connection.createStatement();
            statement.execute(sb.toString(), Statement.RETURN_GENERATED_KEYS);

            ResultSet generatedKeys = statement.getGeneratedKeys();
            generatedKeys.next();
            return generatedKeys.getLong(1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            simpleDb.returnConnection(connection);
        }
    }

    @Override
    public long update() {
        try {
            Statement statement = connection.createStatement();
            return statement.executeUpdate(sb.toString());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            simpleDb.returnConnection(connection);
        }
    }

    @Override
    public long delete() {
        try {
            Statement statement = connection.createStatement();
            return statement.executeUpdate(sb.toString());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            simpleDb.returnConnection(connection);
        }
    }

    @Override
    public List<Map<String, Object>> selectRows() {
        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sb.toString());

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
            simpleDb.returnConnection(connection);
        }
    }

    @Override
    public <T> List<T> selectRows(Class<T> tClass) {
        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sb.toString());
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
            simpleDb.returnConnection(connection);
        }
    }

    @Override
    public Map<String, Object> selectRow() {
        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sb.toString());

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
            simpleDb.returnConnection(connection);
        }
    }

    @Override
    public <T> T selectRow(Class<T> tClass) {
        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sb.toString());

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
            simpleDb.returnConnection(connection);
        }
    }

    @Override
    public LocalDateTime selectDatetime() {
        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sb.toString());

            resultSet.next();
            return resultSet.getTimestamp(1).toLocalDateTime();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            simpleDb.returnConnection(connection);
        }
    }

    @Override
    public long selectLong() {
        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sb.toString());

            resultSet.next();
            return resultSet.getLong(1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            simpleDb.returnConnection(connection);
        }
    }

    @Override
    public String selectString() {
        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sb.toString());

            resultSet.next();
            return resultSet.getString(1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            simpleDb.returnConnection(connection);
        }
    }

    @Override
    public boolean selectBoolean() {
        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sb.toString());

            resultSet.next();
            return resultSet.getBoolean(1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            simpleDb.returnConnection(connection);
        }
    }

    @Override
    public List<Long> selectLongs() {
        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sb.toString());

            ArrayList<Long> result = new ArrayList<>();
            while (resultSet.next()) {
                result.add(resultSet.getLong(1));
            }
            return result;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            simpleDb.returnConnection(connection);
        }
    }

    @Override
    public Sql append(String sql) {
        sb.append(sql + " ");
        return this;
    }

    @Override
    public Sql append(String sql, Object... params) {
        for (Object param : params) {
            sql = sql.replaceFirst("\\?", "'" + param + "'");
        }
        sb.append(sql);

        return this;
    }

    @Override
    public Sql appendIn(String sql, Object... params) {
        StringBuffer stringBuffer = new StringBuffer();
        for (int i = 0; i < params.length; i++) {
            stringBuffer.append("'").append(params[i]).append("'");
            if (i < params.length - 1) {
                stringBuffer.append(", ");
            }
        }
        sql = sql.replaceFirst("\\?", stringBuffer.toString());
        this.sb.append(sql);
        return this;
    }
}
