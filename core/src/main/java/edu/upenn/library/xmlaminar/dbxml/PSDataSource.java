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

package edu.upenn.library.xmlaminar.dbxml;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayDeque;
import java.util.logging.Logger;
import javax.sql.DataSource;

/**
 *
 * @author magibney
 */
public class PSDataSource implements DataSource {
    private final DataSource backing;
    private final ArrayDeque<Connection> conns = new ArrayDeque<Connection>();

    public PSDataSource(DataSource backing) {
        this.backing = backing;
    }

    void returnConnection(Connection conn) {
        synchronized (conns) {
            this.conns.push(conn);
        }
    }
    
    @Override
    public Connection getConnection() throws SQLException {
        Connection c;
        synchronized(conns) {
            c = conns.poll();
        }
        if (c == null) {
            c = new PSCachingConnection(backing.getConnection());
        }
        return new ConnectionWrapper(c, this);
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        Connection c;
        synchronized(conns) {
            c = conns.poll();
        }
        if (c == null) {
            c = new PSCachingConnection(backing.getConnection(username, password));
        }
        return new ConnectionWrapper(c, this);
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
    public PrintWriter getLogWriter() throws SQLException {
        return backing.getLogWriter();
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        backing.setLogWriter(out);
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        backing.setLoginTimeout(seconds);
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return backing.getLoginTimeout();
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return backing.getParentLogger();
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
