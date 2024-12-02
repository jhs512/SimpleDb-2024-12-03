package com.ll.simpleDb;

public class SimpleDb {
    String ip;
    String id;
    String pw;
    String dbName;

    boolean isDev;
    SimpleDb(String ip,String id,String pw,String dbName){
        this.ip = ip;
        this.id = id;
        this.pw = pw;
        this.dbName = dbName;
    }
    void setDevMode(boolean isDev){
        this.isDev = isDev;
    }
    void run(String query){

    }

    void run(String query,String title,String body, boolean isBlind){

    }
    Sql genSql(){
        return new Sql();
    }


}
