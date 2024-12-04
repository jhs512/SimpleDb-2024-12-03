package com.ll;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public interface UpdateResultGetter<T> {
    T getResult(PreparedStatement statement) throws SQLException;
}