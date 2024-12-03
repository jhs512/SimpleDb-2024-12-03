package com.ll.simpleDb;

import java.sql.*;

public class SimpleDb {

    // db driver
    private String dbDriver = "com.mysql.cj.jdbc.Driver";

    // dev Mode 설정
    private boolean devMode;

    // db url
    private String url;

    // sql
    private Sql sql;

    // DB와의 커넥션
    private Connection conn;

    // DB와의 커넥션 쿼리 전달 요소
    private Statement stmt;

    /**
     *
     * 생성자
     *
     * @param ip IP
     * @param id DB ID
     * @param pw DB Password
     * @param dbName DB name
     *
     * @author shjung
     * @since 2024. 12. 02.
     */
    public SimpleDb(String ip, String id, String pw, String dbName) {
        this.url = "jdbc:mysql://" + ip + ":3306/" + dbName;

        try{
            Class.forName(dbDriver);
            conn = DriverManager.getConnection(url, id, pw);
            stmt = conn.createStatement();
        } catch (ClassNotFoundException | SQLException e) {
            throw new RuntimeException(e);
        }
    }


    public void setDevMode(boolean devMode) {
        this.devMode = devMode;
    }

    /**
     *
     * DB 쿼리문 실행
     *
     * @param objects 쿼리문과 입력값
     *
     * @author shjung
     * @since 2024. 12. 02.
     */
    public void run(Object... objects){
        String s = objects[0].toString();

        if(objects.length > 1){
            for (int i = 1; i < objects.length; i++){
                s = s.replaceFirst("\\?", objects[i] instanceof String ? "'" + objects[i] + "'" : objects[i].toString());
            }
        }

        try {
            stmt.execute(s);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    /**
     *
     * sql 객체 생성
     *
     * @return SQL 객체
     *
     * @author shjung
     * @since 2024. 12. 02.
     */
    public Sql genSql(){
        // Sql 객체에 넣을 Statement 객체 새로 생성 후 파라미터로 전달
        try {
            Statement statement = this.conn.createStatement();
            this.sql = new Sql(statement);
            return this.sql;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     *
     * 연결 끊는 함수
     *
     * @author shjung
     * @since 2024. 12. 02.
     */
    public void closeConnection(){
        try {
            this.sql.stat.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
//        try{
//            this.conn.close();
//        } catch (SQLException e) {
//            throw new RuntimeException(e);
//        }
    }

    /**
     *
     * 트랜잭션 시작 함수
     * - 자동 실행 막기 함수
     *
     * @author shjung
     * @since 2024. 12. 02.
     */
    public void startTransaction(){
        try{
            this.conn.setAutoCommit(false);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     *
     * commit 함수
     * - 쿼리문 실행 함수
     *
     * @author shjung
     * @since 2024. 12. 02.
     */
    public void commit(){
        try{
            this.conn.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     *
     * rollback 함수
     * - 쿼리문 취소 함수
     *
     * @author shjung
     * @since 2024. 12. 02.
     */
    public void rollback(){
        try{
            this.conn.rollback();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}