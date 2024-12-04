package com.ll;

import java.sql.SQLException;

public interface DataSource {
    void closeConnection();
    void startTransaction();
    void rollback();
    void commit();
}
