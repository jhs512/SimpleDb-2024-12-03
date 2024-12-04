package org.example;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.logging.Logger;

@RequiredArgsConstructor
public class SimpleDb {

    private final String host;
    private final String user;
    private final String password;
    private final String dbName;

    private boolean onDev;
    private Connection conn;

    public void setDevMode(boolean check) {
        onDev = check;
    }


    public void run(String sql, Object... params) {

        connect();

        try(PreparedStatement pst = conn.prepareStatement(sql)) {
            bindParameters(pst, params);
            pst.executeUpdate();

        } catch (SQLException e) {
            throw new RuntimeException("Failed to execute SQL : " + sql + " Error : " + e.getMessage(), e);
        }
    }

    public Sql genSql() {
        connect();
        return new Sql(conn);
    }

    private void bindParameters(PreparedStatement pst, Object[] params) throws SQLException {
        for (int i = 0; i< params.length; i++) {
            pst.setObject(i+1, params[i]);
        }
    }

    private void connect () {

        if(conn == null) {
            String url = String.format("jdbc:mysql://%s/%s?useSSL=false&allowPublicKeyRetrieval=true", host, dbName);

            try {
                conn = DriverManager.getConnection(url, user, password);
            } catch (SQLException e) {
                throw new RuntimeException("Database Connection failed : ", e);
            }
        }
    }


}
