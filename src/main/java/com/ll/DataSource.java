package com.ll;


public interface DataSource {
    void closeConnection();
    void startTransaction();
    void rollback();
    void commit();
}
