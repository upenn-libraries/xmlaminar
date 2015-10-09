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

import edu.upennlib.paralleltransformer.QueueSourceXMLFilter;
import edu.upennlib.paralleltransformer.TXMLFilter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.regex.Pattern;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.stream.StreamSource;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.xml.sax.InputSource;
import org.xml.sax.XMLFilter;

/**
 *
 * @author magibney
 */
class ProcessCommandFactory extends CommandFactory {
    
    static {
        registerCommandFactory(new ProcessCommandFactory());
    }
    
    @Override
    public Command newCommand(boolean first, boolean last) {
        return new ProcessCommand(first, last);
    }

    @Override
    public String getKey() {
        return "process";
    }

    @Override
    public CommandFactory getConfiguringXMLFilter(boolean first, InitCommand inputBase, CommandType maxType) {
        return null;
    }

    private static class ProcessCommand implements Command<InputCommandFactory.InputCommand> {

        private final boolean first;
        private final boolean last;
        private int recordDepth;
        private final OptionSpec<Integer> recordDepthSpec;
        private File xsl;
        private final OptionSpec<File> xslSpec;
        private String recordIdXPath;
        private final OptionSpec<String> recordIdXPathSpec;
        private boolean subdivide;
        private final OptionSpec subdivideSpec;
        private final OptionSpec verboseSpec;
        private final OptionSpec helpSpec;

        private TXMLFilter txf;

        protected final OptionParser parser;
        protected InputCommandFactory.InputCommand inputBase;


        public ProcessCommand(boolean first, boolean last) {
            this.first = first;
            this.last = last;
            parser = new OptionParser();
            recordDepthSpec = parser.acceptsAll(Flags.DEPTH_ARG, "set record element depth")
                    .withRequiredArg().ofType(Integer.class).defaultsTo(1);
            xslSpec = parser.acceptsAll(Flags.XSL_FILE_ARG, "xsl file defining processing templates").withRequiredArg().ofType(File.class);
            recordIdXPathSpec = parser.acceptsAll(Flags.RECORD_ID_XPATH_ARG, "xpath specifying record id location").withRequiredArg().ofType(String.class);
            subdivideSpec = parser.acceptsAll(Flags.SUBDIVIDE_ARG, "define behavior on processing failure");
            verboseSpec = parser.acceptsAll(Flags.VERBOSE_ARG, "be more verbose");
            helpSpec = parser.acceptsAll(Flags.HELP_ARG, "show help").forHelp();
        }

        protected boolean init(OptionSet options, InputCommandFactory.InputCommand inputBase) {
            this.inputBase = inputBase;
            if (options.has(helpSpec)) {
                return false;
            }
            recordDepth = options.valueOf(recordDepthSpec);
            xsl = options.valueOf(xslSpec);
            recordIdXPath = options.valueOf(recordIdXPathSpec);
            subdivide = options.has(subdivideSpec);
            return true;
        }
        
        private static final boolean EXPECT_INPUT = true;
        
        @Override
        public XMLFilter getXMLFilter(ArgFactory arf, InputCommandFactory.InputCommand inputBase, CommandType maxType) {
            if (txf != null) {
                return txf;
            }
            if (!init(parser.parse(arf.getArgs(parser.recognizedOptions().keySet())), inputBase)) {
                return null;
            }
            CommandFactory.conditionalInit(first, inputBase, EXPECT_INPUT);
            try {
                txf = new TXMLFilter(new StreamSource(xsl), recordIdXPath, subdivide, recordDepth);
            } catch (TransformerConfigurationException ex) {
                throw new RuntimeException(ex);
            }
            if (first && inputBase.filesFrom != null) {
                txf.setInputType(QueueSourceXMLFilter.InputType.indirect);
                if (inputBase.delim != null) {
                    txf.setDelimiterPattern(Pattern.compile(inputBase.delim, Pattern.LITERAL));
                }
            }
            return txf;
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
