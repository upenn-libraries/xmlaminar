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

import edu.upennlib.paralleltransformer.InputSplitter;
import edu.upennlib.paralleltransformer.JoiningXMLFilter;
import edu.upennlib.xmlutils.dbxml.BinaryMARCXMLReader;
import edu.upennlib.xmlutils.dbxml.SQLXMLReader;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.xml.sax.XMLFilter;

/**
 *
 * @author magibney
 */
public class MARCRSXMLReaderCommandFactory extends CommandFactory {
    static {
        registerCommandFactory(new MARCRSXMLReaderCommandFactory());
    }

    @Override
    public Command newCommand(boolean first, boolean last) {
//        if (!first) {
//            throw new IllegalArgumentException("type "+MARCRSXMLReaderCommandFactory.class+" must be first in pipeline");
//        }
        return new MARCRSXMLReaderCommand(first, last);
    }

    @Override
    public String getKey() {
        return "marcdb-to-xml";
    }

    @Override
    public CommandFactory getConfiguringXMLFilter(boolean first, InitCommand inputBase, CommandType maxType) {
        return null;
    }
    
    private static class MARCRSXMLReaderCommand extends SQLXMLReaderCommand {

        private boolean initialized = false;
        private BinaryMARCXMLReader mxr;
        private XMLFilter ret;
        protected String marcBinaryFieldLabel;
        private final OptionSpec<String> marcBinaryFieldLabelSpec;

        public MARCRSXMLReaderCommand(boolean first, boolean last) {
            super(first, last);
            marcBinaryFieldLabelSpec = parser.acceptsAll(Flags.MARC_FIELD_LABEL_ARG, "label of field containing chunked binary marc")
                    .withRequiredArg().ofType(String.class);
        }

        @Override
        protected boolean init(OptionSet options, InputCommandFactory.InputCommand inputBase) {
            boolean ret = super.init(options, inputBase);
            marcBinaryFieldLabel = options.valueOf(marcBinaryFieldLabelSpec);
            return ret;
        }

        private static final boolean EXPECT_INPUT = false; // for now, change for parameterized SQL
        
        @Override
        public XMLFilter getXMLFilter(ArgFactory arf, InputCommandFactory.InputCommand inputBase, CommandType maxType) {
            String[] args = arf.getArgs(parser.recognizedOptions().keySet());
            if (initialized) {
                return ret;
            } else {
                CommandFactory.conditionalInit(first, inputBase, EXPECT_INPUT);
                initialized = true;
                if (!init(parser.parse(args), inputBase)) {
                    return null;
                } else {
                    mxr = new BinaryMARCXMLReader(batchSize, lookaheadFactor);
//                    if (lookaheadFactor > 0) {
//                        mxr.setExecutor(Executors.newCachedThreadPool(DAEMON_THREAD_FACTORY));
//                    }
                    mxr.setSuppressParameterizedClause(suppressParamClause);
                    mxr.setName(name);
                    mxr.setIdFieldLabels(parseIdFieldLabels(idFieldLabels));
                    mxr.setOutputFieldLabels(new String[]{marcBinaryFieldLabel});
                    if (dataSourceName != null) {
                        mxr.setDataSourceName(dataSourceName);
                    } else {
                        mxr.setDataSource(SQLXMLReader.newDataSource(connectionConfigFile));
                    }
                    mxr.setSql(sql);
                    if (expectPresplitInput || !mxr.isParameterized()) {
                        ret = mxr;
                    } else if (!first) {
                        PivotXMLFilter pivot = new PivotXMLFilter(mxr, batchSize, lookaheadFactor);
                        ret = pivot;
                    } else {
                        JoiningXMLFilter joiner = new JoiningXMLFilter(true);
                        joiner.setParent(mxr);
                        joiner.setIteratorWrapper(new InputSplitter(batchSize, lookaheadFactor));
                        ret = joiner;
                    }
                }
            }
            return ret;
        }

        @Override
        public boolean handlesOutput() {
            return false;
        }

        @Override
        public InitCommand inputHandler() {
            return inputBase;
        }

    }

    private static final Pattern COMMA_SPLIT = Pattern.compile("\\s*([^,]+)\\s*(,|$)");
    
    private static String[] parseIdFieldLabels(String idFields) {
        Matcher m = COMMA_SPLIT.matcher(idFields);
        ArrayList<String> fieldList = new ArrayList<String>();
        while (m.find()) {
            fieldList.add(m.group(1));
        }
        return fieldList.toArray(new String[fieldList.size()]);
    }
}
