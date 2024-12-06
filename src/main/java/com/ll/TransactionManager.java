package com.ll;

import java.sql.SQLException;

public final class TransactionManager {
    public static void startTransaction(Sql sql){
        try{
            sql.getConnection().setAutoCommit(false);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public  static void commit(Sql sql){
        try{
            sql.getConnection().commit();
            sql.getConnection().setAutoCommit(true);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public  static void rollback(Sql sql){
        try{
            sql.getConnection().rollback();
            sql.getConnection().setAutoCommit(true);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
