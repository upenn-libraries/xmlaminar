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

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

/**
 *
 * @author magibney
 */
public enum SQLParam {
    
    INTEGER(Types.INTEGER, new Int(), Integer.class),
    BIGINT(Types.BIGINT, new BigInt(), Long.class);

    public PreparedStatement init(PreparedStatement ps, int index, String val) throws SQLException {
        return init(ps, index, psInit.convert(val));
    }
    
    public <T> PreparedStatement init(PreparedStatement ps, int index, T val) throws SQLException {
        return psInit.init(ps, index, val);
    }
    
    public int getSQLType() {
        return type;
    }
    
    public Class<?> getType() {
        return clazz;
    }
    
    private final int type;
    private final PreparedStatementInit psInit;
    private final Class<?> clazz;
    
    private <T> SQLParam(int type, PreparedStatementInit<T> psInit, Class<T> clazz) {
        this.type = type;
        this.psInit = psInit;
        this.clazz = clazz;
    }
    
    private static interface PreparedStatementInit<T> {
        T convert(String val);
        PreparedStatement init(PreparedStatement ps, int index, T val) throws SQLException;
    }
    
    private static class Int implements PreparedStatementInit<Integer> {

        @Override
        public PreparedStatement init(PreparedStatement ps, int index, Integer val) throws SQLException {
            ps.setInt(index, val);
            return ps;
        }

        @Override
        public Integer convert(String val) {
            return Integer.parseInt(val);
        }
        
    }

    private static class BigInt implements PreparedStatementInit<Long> {

        @Override
        public PreparedStatement init(PreparedStatement ps, int index, Long val) throws SQLException {
            ps.setLong(index, val);
            return ps;
        }

        @Override
        public Long convert(String val) {
            return Long.parseLong(val);
        }

    }
    
    
}
