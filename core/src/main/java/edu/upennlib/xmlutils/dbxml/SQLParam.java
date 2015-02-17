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
    
    INTEGER(Types.INTEGER, new Int()),
    BIGINT(Types.BIGINT, new BigInt());

    public PreparedStatement init(PreparedStatement ps, int index, String val) throws SQLException {
        return psInit.init(ps, index, val);
    }
    
    public int getType() {
        return type;
    }
    
    private final int type;
    private final PreparedStatementInit psInit;
    
    private SQLParam(int type, PreparedStatementInit psInit) {
        this.type = type;
        this.psInit = psInit;
    }
    
    private static interface PreparedStatementInit {
        PreparedStatement init(PreparedStatement ps, int index, String val) throws SQLException;
    }
    
    private static class Int implements PreparedStatementInit {

        @Override
        public PreparedStatement init(PreparedStatement ps, int index, String val) throws SQLException {
            ps.setInt(index, Integer.parseInt(val));
            return ps;
        }
        
    }

    private static class BigInt implements PreparedStatementInit {

        @Override
        public PreparedStatement init(PreparedStatement ps, int index, String val) throws SQLException {
            ps.setLong(index, Long.parseLong(val));
            return ps;
        }

    }
    
    
}
