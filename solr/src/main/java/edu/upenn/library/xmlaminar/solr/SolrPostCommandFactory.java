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

package edu.upenn.library.xmlaminar.solr;

import edu.upenn.library.xmlaminar.cli.Command;
import edu.upenn.library.xmlaminar.cli.CommandFactory;
import edu.upenn.library.xmlaminar.cli.CommandType;
import edu.upenn.library.xmlaminar.cli.Flags;
import edu.upenn.library.xmlaminar.cli.InitCommand;
import edu.upenn.library.xmlaminar.cli.InputCommandFactory;
import edu.upenn.library.xmlaminar.parallel.QueueSourceXMLFilter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.regex.Pattern;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrServer;
import org.xml.sax.InputSource;
import org.xml.sax.XMLFilter;

/**
 *
 * @author magibney
 */
class SolrPostCommandFactory extends CommandFactory {
    
    static {
        registerCommandFactory(new SolrPostCommandFactory());
    }
    
    @Override
    public Command newCommand(boolean first, boolean last) {
        return new SplitCommand(first, last);
    }

    @Override
    public String getKey() {
        return "solrpost";
    }

    @Override
    public CommandFactory getConfiguringXMLFilter(boolean first, InitCommand inputBase, CommandType maxType) {
        return null;
    }

    private static class SplitCommand implements Command<InputCommandFactory.InputCommand> {

        private static final int DEFAULT_QUEUE_SIZE = 10;
        private static final Collection<String> QUEUE_SIZE_ARG = Arrays.asList("queue-size");
        private int queueSize;
        private final OptionSpec<Integer> threadCountSpec;
        private static final int DEFAULT_THREAD_COUNT = 4;
        private static final Collection<String> THREAD_COUNT_ARG = Arrays.asList("thread-count");
        private int threadCount;
        private final OptionSpec<String> solrUrlSpec;
        private static final Collection<String> SOLR_URL_ARG = Arrays.asList("solr-url");
        private String solrURL;
        private final OptionSpec<Integer> queueSizeSpec;

        private final OptionSpec verboseSpec;
        private final OptionSpec helpSpec;

        protected final OptionParser parser;

        protected final boolean first;
        protected final boolean last;
        
        private InputCommandFactory.InputCommand inputBase;

        public SplitCommand(boolean first, boolean last) {
            this.first = first;
            this.last = last;
            parser = new OptionParser();
            solrUrlSpec = parser.acceptsAll(SOLR_URL_ARG, "solr update server url")
                    .withRequiredArg().ofType(String.class);
            queueSizeSpec = parser.acceptsAll(QUEUE_SIZE_ARG, "solr update queue size")
                    .withRequiredArg().ofType(Integer.class).defaultsTo(DEFAULT_QUEUE_SIZE);
            threadCountSpec = parser.acceptsAll(THREAD_COUNT_ARG, "solr update thread count")
                    .withRequiredArg().ofType(Integer.class).defaultsTo(DEFAULT_THREAD_COUNT);
            verboseSpec = parser.acceptsAll(Flags.VERBOSE_ARG, "be more verbose");
            helpSpec = parser.acceptsAll(Flags.HELP_ARG, "show help").forHelp();
        }

        protected boolean init(OptionSet options, InputCommandFactory.InputCommand inputBase) {
            this.inputBase = inputBase;
            if (options.has(helpSpec)) {
                return false;
            }
            solrURL = options.valueOf(solrUrlSpec);
            threadCount = options.valueOf(threadCountSpec);
            queueSize = options.valueOf(queueSizeSpec);
            return true;
        }
        
        private static final boolean EXPECT_INPUT = true;
        
        @Override
        public XMLFilter getXMLFilter(ArgFactory arf, InputCommandFactory.InputCommand inputBase, CommandType maxType) {
            if (!init(parser.parse(arf.getArgs(parser.recognizedOptions().keySet())), inputBase)) {
                return null;
            }
            CommandFactory.conditionalInit(first, inputBase, EXPECT_INPUT);
            SAXSolrPoster ssp = new SAXSolrPoster();
            ssp.setServer(new ConcurrentUpdateSolrServer(solrURL, queueSize, threadCount));
            if (first && inputBase.getFilesFrom() != null) {
                ssp.setInputType(QueueSourceXMLFilter.InputType.indirect);
                if (inputBase.getDelim() != null) {
                    ssp.setDelimiterPattern(Pattern.compile(inputBase.getDelim(), Pattern.LITERAL));
                }
            }
            return ssp;
        }

        @Override
        public CommandType getCommandType() {
            return CommandType.PASS_THROUGH;
        }

        @Override
        public File getInputBase() {
            return inputBase.getInputBase();
        }

        @Override
        public boolean handlesOutput() {
            return true;
        }

        @Override
        public InitCommand inputHandler() {
            return inputBase;
        }

        @Override
        public InputSource getInput() throws FileNotFoundException {
            if (first) {
                return inputBase.getInput();
            } else {
                return null;
            }
        }

        @Override
        public void printHelpOn(OutputStream out) {
            try {
                parser.printHelpOn(out);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

}
