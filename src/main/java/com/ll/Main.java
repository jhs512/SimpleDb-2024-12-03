package com.ll;


import java.sql.SQLException;

public class Main {
    public static void main(String[] args) throws SQLException {
        new SimpleDb("localhost", "root", "lldj123414", "simpleDb__test");
    }
}