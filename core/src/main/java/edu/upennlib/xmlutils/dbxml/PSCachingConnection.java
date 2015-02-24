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

package edu.upennlib.xmlutils.dbxml;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

/**
 *
 * @author magibney
 */
class PSCachingConnection implements Connection {
    
    private final Connection backing;
    private String sql;
    private PreparedStatement ps;

    PSCachingConnection(Connection conn) {
        this.backing = conn;
    }

    @Override
    public Statement createStatement() throws SQLException {
        return backing.createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        if (!sql.equals(this.sql)) {
            ps = backing.prepareStatement(sql);
            this.sql = sql;
        } else {
            System.err.println("XXX reusing ps!");
            ps.clearParameters();
        }
        return ps;
    }
    
    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        return backing.prepareCall(sql);
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        return backing.nativeSQL(sql);
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        backing.setAutoCommit(autoCommit);
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return backing.getAutoCommit();
    }

    @Override
    public void commit() throws SQLException {
        backing.commit();
    }

    @Override
    public void rollback() throws SQLException {
        backing.rollback();
    }

    @Override
    public void close() throws SQLException {
        ps = null;
        sql = null;
        backing.close();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return backing.isClosed();
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return backing.getMetaData();
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        backing.setReadOnly(readOnly);
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return backing.isReadOnly();
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        backing.setCatalog(catalog);
    }

    @Override
    public String getCatalog() throws SQLException {
        return backing.getCatalog();
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        backing.setTransactionIsolation(level);
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return backing.getTransactionIsolation();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return backing.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        backing.clearWarnings();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return backing.createStatement(resultSetType, resultSetConcurrency);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return backing.prepareStatement(sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return backing.prepareCall(sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return backing.getTypeMap();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        backing.setTypeMap(map);
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        backing.setHoldability(holdability);
    }

    @Override
    public int getHoldability() throws SQLException {
        return backing.getHoldability();
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        return backing.setSavepoint();
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        return backing.setSavepoint(name);
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        backing.rollback(savepoint);
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        backing.releaseSavepoint(savepoint);
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return backing.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return backing.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return backing.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return backing.prepareStatement(sql, autoGeneratedKeys);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return backing.prepareStatement(sql, columnIndexes);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        return backing.prepareStatement(sql, columnNames);
    }

    @Override
    public Clob createClob() throws SQLException {
        return backing.createClob();
    }

    @Override
    public Blob createBlob() throws SQLException {
        return backing.createBlob();
    }

    @Override
    public NClob createNClob() throws SQLException {
        return backing.createNClob();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        return backing.createSQLXML();
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return backing.isValid(timeout);
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        backing.setClientInfo(name, value);
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        backing.setClientInfo(properties);
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return backing.getClientInfo(name);
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return backing.getClientInfo();
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return backing.createArrayOf(typeName, elements);
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return backing.createStruct(typeName, attributes);
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        backing.setSchema(schema);
    }

    @Override
    public String getSchema() throws SQLException {
        return backing.getSchema();
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        backing.abort(executor);
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        backing.setNetworkTimeout(executor, milliseconds);
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return backing.getNetworkTimeout();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return iface.cast(backing);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return backing.getClass() == iface;
    }

    @Override
    public int hashCode() {
        return backing.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return backing.equals(obj);
    }

    @Override
    public String toString() {
        return backing.toString();
    }
    
}
