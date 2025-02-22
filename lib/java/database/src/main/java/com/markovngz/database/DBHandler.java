package com.markovngz.database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DBHandler {
    private Connection connection;

    private final String jdbcUrl;
    private String user;
    private String password;
   

    public DBHandler(String driver , String host, int port, String database, String user, String password) {
        this.jdbcUrl = String.format("jdbc:%s://%s:%d/%s", driver, host, port, database);
        this.user = user;
        this.password = password;
    }

    public void connect() throws SQLException {
        if (connection == null || connection.isClosed()) {
            connection = DriverManager.getConnection(jdbcUrl, user, password);
        }
    }

    public void disconnect() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    public ResultSet executeQuery(String query) throws SQLException {
        connect();
        PreparedStatement stmt = connection.prepareStatement(query);
        return stmt.executeQuery();
    }

    public int executeUpdate(String query) throws SQLException {
        connect();
        PreparedStatement stmt = connection.prepareStatement(query);
        return stmt.executeUpdate();
    }
    
    public boolean isConnected() throws SQLException {
        return connection != null && !connection.isClosed();
    }
}