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
import java.util.concurrent.ThreadFactory;
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
    private static final Logger LOG = LoggerFactory.getLogger(SQLXMLReaderCommand.class);
    protected String name;
    private final OptionSpec<String> nameSpec;
    protected File connectionConfigFile;
    private final OptionSpec<File> connectionConfigFileSpec;
    protected String dataSourceName;
    private final OptionSpec<String> dataSourceNameSpec;
    protected String sql;
    private final OptionSpec<String> sqlSpec;
    protected String idFieldLabels;
    private final OptionSpec<String> idFieldLabelsSpec;
    protected Integer batchSize;
    private final OptionSpec<Integer> batchSizeSpec;
    protected Integer lookaheadFactor;
    private final OptionSpec<Integer> lookaheadFactorSpec;
    protected boolean expectPresplitInput;
    private final OptionSpec expectPresplitInputSpec;
    protected boolean suppressParamClause;
    private final OptionSpec suppressParamClauseSpec;
    
    protected static final ThreadFactory DAEMON_THREAD_FACTORY = new ThreadFactory() {

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setDaemon(true);
            return t;
        }
    };

    public SQLXMLReaderCommand(boolean first, boolean last) {
        super(first, last);
        expectPresplitInputSpec = parser.acceptsAll(Flags.EXPECT_PRESPLIT_INPUT_ARG);
        suppressParamClauseSpec = parser.acceptsAll(Flags.SUPPRESS_PARAM_CLAUSE_ARG);
        nameSpec = parser.acceptsAll(Flags.NAME_ARG, "optional name to identify this source")
                .withRequiredArg().ofType(String.class);
        connectionConfigFileSpec = parser.acceptsAll(Flags.CONNECTION_CONFIG_FILE_ARG, 
                "path to file specifying host, sid, user, pwd for connection")
                .withRequiredArg().ofType(File.class);
        sqlSpec = parser.acceptsAll(Flags.SQL_ARG).withRequiredArg().ofType(String.class);
        dataSourceNameSpec = parser.acceptsAll(Flags.DATA_SOURCE_NAME_ARG).withRequiredArg().ofType(String.class);
        batchSizeSpec = parser.acceptsAll(Flags.SIZE_ARG).withRequiredArg().ofType(Integer.class).defaultsTo(6);
        lookaheadFactorSpec = parser.acceptsAll(Flags.LOOKAHEAD_FACTOR_ARG, "prefetch input chunks asynchronously").withRequiredArg().ofType(Integer.class).defaultsTo(0);
        idFieldLabelsSpec = parser.acceptsAll(Flags.ID_FIELD_LABELS_ARG, "ordered list of"
                + " sorted field names by which to group output")
                .withRequiredArg().ofType(String.class);
    }

    @Override
    protected boolean init(OptionSet options, InputCommandFactory.InputCommand inputBase) {
        boolean ret = super.init(options, inputBase);
        name = options.valueOf(nameSpec);
        sql = options.valueOf(sqlSpec);
        idFieldLabels = options.valueOf(idFieldLabelsSpec);
        batchSize = options.valueOf(batchSizeSpec);
        lookaheadFactor = options.valueOf(lookaheadFactorSpec);
        expectPresplitInput = options.has(expectPresplitInputSpec);
        suppressParamClause = options.has(suppressParamClauseSpec);
        if (options.has(dataSourceNameSpec)) {
            dataSourceName = options.valueOf(dataSourceNameSpec);
        } else if (options.has(connectionConfigFileSpec)) {
            connectionConfigFile = options.valueOf(connectionConfigFileSpec);
        }
        return ret;
    }
    
    private Properties parseConnectionConfig(File configFile, Properties def) {
        InputStream in = null;
        try {
            in = InputCommandFactory.InputCommand.configureInputSource(new InputSource(), configFile).getByteStream();
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
        return inputBase.getInputBase();
    }

    @Override
    public CommandType getCommandType() {
        return CommandType.PASS_THROUGH;
    }

}
