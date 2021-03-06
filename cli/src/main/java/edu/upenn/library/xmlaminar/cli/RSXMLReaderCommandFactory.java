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

package edu.upenn.library.xmlaminar.cli;

import edu.upenn.library.xmlaminar.parallel.InputSplitter;
import edu.upenn.library.xmlaminar.parallel.JoiningXMLFilter;
import edu.upenn.library.xmlaminar.dbxml.RSXMLReader;
import edu.upenn.library.xmlaminar.dbxml.SQLXMLReader;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.xml.sax.XMLFilter;

/**
 *
 * @author magibney
 */
public class RSXMLReaderCommandFactory extends CommandFactory {
    static {
        registerCommandFactory(new RSXMLReaderCommandFactory());
    }

    @Override
    public Command newCommand(boolean first, boolean last) {
//        if (!first) {
//            throw new IllegalArgumentException("type "+RSXMLReaderCommandFactory.class+" must be first in pipeline");
//        }
        return new RSXMLReaderCommand(first, last);
    }

    @Override
    public String getKey() {
        return "db-to-xml";
    }

    @Override
    public CommandFactory getConfiguringXMLFilter(boolean first, InitCommand inputBase, CommandType maxType) {
        return null;
    }
    
    private static class RSXMLReaderCommand extends SQLXMLReaderCommand {

        private boolean initialized = false;
        private RSXMLReader rsxr;
        private XMLFilter ret;

        public RSXMLReaderCommand(boolean first, boolean last) {
            super(first, last);
        }
        
        private static final boolean EXPECT_INPUT = false; // for now, change for parameterized SQL

        @Override
        public XMLFilter getXMLFilter(ArgFactory arf, InputCommandFactory.InputCommand inputBase, CommandType maxType) {
            if (initialized) {
                return ret;
            } else {
                CommandFactory.conditionalInit(first, inputBase, EXPECT_INPUT);
                initialized = true;
                if (!init(parser.parse(arf.getArgs(parser.recognizedOptions().keySet())), inputBase)) {
                    return null;
                } else {
                    rsxr = new RSXMLReader(batchSize, lookaheadFactor);
//                    if (lookaheadFactor > 0) {
//                        rsxr.setExecutor(Executors.newCachedThreadPool(DAEMON_THREAD_FACTORY));
//                    }
                    rsxr.setSuppressParameterizedClause(suppressParamClause);
                    rsxr.setName(name);
                    rsxr.setIdFieldLabels(parseIdFieldLabels(idFieldLabels));
                    if (dataSourceName != null) {
                        rsxr.setDataSourceName(dataSourceName);
                    } else {
                        rsxr.setDataSource(SQLXMLReader.newDataSource(connectionConfigFile));
                    }
                    rsxr.setSql(sql);
                    if (expectPresplitInput || !rsxr.isParameterized()) {
                        ret = rsxr;
                    } else if (!first) {
                        PivotXMLFilter pivot = new PivotXMLFilter(rsxr, batchSize, lookaheadFactor);
                        ret = pivot;
                    } else {
                        JoiningXMLFilter joiner = new JoiningXMLFilter(true);
                        joiner.setParent(rsxr);
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
