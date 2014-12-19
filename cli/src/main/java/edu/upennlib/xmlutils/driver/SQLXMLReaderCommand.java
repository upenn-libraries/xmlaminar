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

package edu.upennlib.xmlutils.driver;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;

/**
 *
 * @author magibney
 */
public abstract class SQLXMLReaderCommand extends MultiOutCommand {
    public static final String HOST_KEY = "host";
    public static final String SID_KEY = "sid";
    public static final String USER_KEY = "user";
    public static final String PASSWORD_KEY = "password";
    private static final Logger LOG = LoggerFactory.getLogger(SQLXMLReaderCommand.class);
    protected String name;
    private final OptionSpec<String> nameSpec;
    protected String host;
    protected String sid;
    protected String user;
    protected String password;
    private final OptionSpec<File> connectionConfigFileSpec;
    protected String sql;
    private final OptionSpec<String> sqlSpec;
    protected String idFieldLabels;
    private final OptionSpec<String> idFieldLabelsSpec;

    public SQLXMLReaderCommand(boolean first, boolean last) {
        super(first, last);
        nameSpec = parser.acceptsAll(Flags.NAME_ARG, "optional name to identify this source")
                .withRequiredArg().ofType(String.class);
        connectionConfigFileSpec = parser.acceptsAll(Flags.CONNECTION_CONFIG_FILE_ARG, 
                "path to file specifying host, sid, user, pwd for connection")
                .withRequiredArg().ofType(File.class);
        sqlSpec = parser.acceptsAll(Flags.SQL_ARG).withRequiredArg().ofType(String.class);
        idFieldLabelsSpec = parser.acceptsAll(Flags.ID_FIELD_LABELS_ARG, "ordered list of"
                + " sorted field names by which to group output")
                .withRequiredArg().ofType(String.class);
    }

    @Override
    protected boolean init(OptionSet options) {
        boolean ret = super.init(options);
        name = options.valueOf(nameSpec);
        sql = options.valueOf(sqlSpec);
        idFieldLabels = options.valueOf(idFieldLabelsSpec);
        if (options.has(connectionConfigFileSpec)) {
            Properties connectionConfig = parseConnectionConfig(options.valueOf(connectionConfigFileSpec), null);
            host = connectionConfig.getProperty(HOST_KEY);
            sid = connectionConfig.getProperty(SID_KEY);
            user = connectionConfig.getProperty(USER_KEY);
            password = connectionConfig.getProperty(PASSWORD_KEY);
        }
        return ret;
    }
    
    private Properties parseConnectionConfig(File configFile, Properties def) {
        InputStream in = null;
        try {
            in = configureInputSource(new InputSource(), configFile).getByteStream();
            Properties ret = new Properties(def);
            ret.load(in);
            return ret;
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException ex) {
                    LOG.error("error closing connection config file", ex);
                    // allow initial error to be thrown
                }
            }
        }
    }

    @Override
    public File getInputBase() {
        if (input == null) {
            return new File("");
        } else if ("-".equals(input.getPath())) {
            return null;
        } else if (!input.isDirectory()) {
            return null;
        } else {
            return input;
        }
    }

    @Override
    public CommandType getCommandType() {
        return CommandType.PASS_THROUGH;
    }

}
