/*
 * Copyright 2011-2015 The Trustees of the University of Pennsylvania
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.upennlib.ingestor.sax.utils;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import oracle.jdbc.driver.OracleConnection;

import org.apache.log4j.Logger;

public class Connection {

    private java.sql.Connection connection;
    private static final int DEFAULT_ROW_PREFETCH = 50;
    private int rowPrefetch = DEFAULT_ROW_PREFETCH;
    private String host;
    private String sid;
    private String sql;
    private String user;
    private String pwd;
    private Logger logger;

    public Connection() throws ConnectionException {
        logger = Logger.getLogger(this.getClass());

        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
        } catch (Exception e) {
            throw new ConnectionException("Could not load ojdbc driver.");
        }
    }

    public void setHost(String host) {
        logger.trace("set host: " + host);
        this.host = host;
    }

    public String getHost() {
        return host;
    }

    public void setSid(String sid) {
        logger.trace("set sid: " + sid);
        this.sid = sid;
    }

    public String getSid() {
        return sid;
    }

    public void setSql(String sql) {
        logger.trace("set sql: " + sql);
        this.sql = sql;
    }

    public String getSql() {
        return sql;
    }

    public void setUser(String user) {
        logger.trace("set user: " + user);
        this.user = user;
    }

    public String getUser() {
        return user;
    }

    public void setPwd(String pwd) {
        logger.trace("set pwd: " + pwd);
        this.pwd = pwd;
    }

    public String getPwd() {
        return pwd;
    }

    public void setRowPrefetch(int rowPrefetch) {
        logger.trace("set pwd: " + pwd);
        this.rowPrefetch = rowPrefetch;
    }

    public int getRowPrefetch() {
        return rowPrefetch;
    }

    public void connect() throws ConnectionException {
        logger.trace("Attempting to establish connection");
        String connectionString = "jdbc:oracle:thin:@" + host + ":1521:" + sid;
        try {
            connection = DriverManager.getConnection(connectionString, user, pwd);
            //((OracleConnection)connection).setDefaultRowPrefetch(rowPrefetch);
        } catch (SQLException e) {
            throw new ConnectionException("Error in connection initialization: " + e.getMessage());
        }
        logger.trace("Connection appears to have been established successfully.");
    }

    public ResultSet getResultSet() throws ConnectionException {
        if (connection == null) {
            logger.trace("getResultSet() called while connection == null; attempting to open connection.");
            connect();
        }

        Statement statement;
        ResultSet results;

        logger.trace("Attempting to create statement and execute sql.");
        try {
            statement = connection.createStatement();
            results = statement.executeQuery(sql);
        } catch (Exception e) {
            throw new ConnectionException("Error creating/executing SQL: " + e.getMessage());
        }
        logger.trace("Executed statement without exception.");

        if (results == null) {
            throw new ConnectionException("ResultSet is null; no results returned.");
        }

        logger.trace("ResultSet is not null.  Returning.");

        return results;
    }

    public void close() {
        logger.trace("Attempting to close connection.");
        try {
            connection.close();
        } catch (SQLException e) {
            logger.warn("Exception thrown closing the DB connection: " + e.getMessage());
        }
        logger.trace("Connection closed without exception.");
    }

    public static void main(String[] args) throws ConnectionException, SQLException {
        String user = args[0];
        String pwd = args[1];
        String sql = args[2];
        Connection conn = new Connection();
        conn.setHost("[host_or_ip]");
        conn.setPwd(pwd);
        conn.setSid("[sid]");
        conn.setSql(sql);
        conn.setUser(user);
        conn.connect();
        ResultSet results = conn.getResultSet();
        ResultSetMetaData md = results.getMetaData();
        int cols = md.getColumnCount();
        int rowCount = 0;
        while (results.next()) {
            rowCount++;
            System.out.println("row : " + rowCount);
            for (int i = 1; i <= cols; i++) {
                System.out.println(md.getColumnLabel(i) + " : " + results.getString(i));
            }
            System.out.println("-------------------------");
        }

    }
}
