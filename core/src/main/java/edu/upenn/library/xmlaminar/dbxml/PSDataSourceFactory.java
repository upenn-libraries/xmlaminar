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

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import javax.sql.DataSource;

/**
 *
 * @author magibney
 */
public class PSDataSourceFactory implements DataSourceFactory {

    private final Map<String, Map<String, DataSource>> backingDss = new HashMap<String, Map<String, DataSource>>();
    
    private final Map<String, File> dsConfigs;
    
    public PSDataSourceFactory(Map<String, File> dsConfigs) {
        this.dsConfigs = new HashMap<String, File>(dsConfigs);
    }
    
    @Override
    public DataSource newDataSource(String name, String psSQL) {
        DataSource ds;
        synchronized(backingDss) {
            Map<String, DataSource> psDs = backingDss.get(name);
            if (psDs == null) {
                psDs = new HashMap<String, DataSource>();
                backingDss.put(name, psDs);
                DataSource backing = SQLXMLReader.newDataSource(dsConfigs.get(name));
                psDs.put(null, backing);
                ds = new PSDataSource(backing);
                psDs.put(psSQL, ds);
            } else if ((ds = psDs.get(psSQL)) == null) {
                ds = new PSDataSource(psDs.get(null));
                psDs.put(psSQL, ds);
            }
        }
        return ds;
    }
    
}
