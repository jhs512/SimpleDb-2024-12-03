package com.ll;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public interface SelectResultGetter<T> {
    T getResult(PreparedStatement statement, ResultSet resultSet) throws SQLException;
}
