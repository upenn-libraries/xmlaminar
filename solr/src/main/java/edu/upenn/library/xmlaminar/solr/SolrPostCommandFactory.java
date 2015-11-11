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
import edu.upenn.library.xmlaminar.parallel.LevelSplittingXMLFilter;
import edu.upenn.library.xmlaminar.parallel.QueueSourceXMLFilter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.regex.Pattern;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
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

        private int chunkSize;
        private final OptionSpec<Integer> chunkSizeSpec;
        private int recordDepth;
        private final OptionSpec<Integer> recordDepthSpec;

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
            recordDepthSpec = parser.acceptsAll(Flags.DEPTH_ARG, "set record element depth")
                    .withRequiredArg().ofType(Integer.class).defaultsTo(1);
            chunkSizeSpec = parser.acceptsAll(Flags.SIZE_ARG, "size (in records) of output files (for split) "
                    + "or processing chunks (for process)").withRequiredArg().ofType(Integer.class)
                    .defaultsTo(100);
            verboseSpec = parser.acceptsAll(Flags.VERBOSE_ARG, "be more verbose");
            helpSpec = parser.acceptsAll(Flags.HELP_ARG, "show help").forHelp();
        }

        protected boolean init(OptionSet options, InputCommandFactory.InputCommand inputBase) {
            this.inputBase = inputBase;
            if (options.has(helpSpec)) {
                return false;
            }
            recordDepth = options.valueOf(recordDepthSpec);
            chunkSize = options.valueOf(chunkSizeSpec);
            return true;
        }
        
        private static final boolean EXPECT_INPUT = true;
        
        @Override
        public XMLFilter getXMLFilter(ArgFactory arf, InputCommandFactory.InputCommand inputBase, CommandType maxType) {
            if (!init(parser.parse(arf.getArgs(parser.recognizedOptions().keySet())), inputBase)) {
                return null;
            }
            CommandFactory.conditionalInit(first, inputBase, EXPECT_INPUT);
            LevelSplittingXMLFilter splitter = new LevelSplittingXMLFilter(recordDepth, chunkSize);
            if (first && inputBase.getFilesFrom() != null) {
                splitter.setInputType(QueueSourceXMLFilter.InputType.indirect);
                if (inputBase.getDelim() != null) {
                    splitter.setDelimiterPattern(Pattern.compile(inputBase.getDelim(), Pattern.LITERAL));
                }
            }
            return splitter;
        }

        @Override
        public CommandType getCommandType() {
            return CommandType.SPLIT;
        }

        @Override
        public File getInputBase() {
            return inputBase.getInputBase();
        }

        @Override
        public boolean handlesOutput() {
            return false;
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
