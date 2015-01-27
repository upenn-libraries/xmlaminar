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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.xml.sax.InputSource;
import org.xml.sax.XMLFilter;

/**
 *
 * @author magibney
 */
class InputCommandFactory extends CommandFactory {

    private static final String KEY = "input";

    static {
        registerCommandFactory(new InputCommandFactory());
    }

    @Override
    public Command newCommand(boolean first, boolean last) {
        return new InputCommand(first, last);
    }

    @Override
    public String getKey() {
        return KEY;
    }

    @Override
    public CommandFactory getConfiguringXMLFilter(boolean first, Command inputBase, CommandType maxType) {
        return null;
    }

    public static class InputCommand implements Command {

        protected File input;
        protected OptionSpec<File> inputFileSpec;
        protected File filesFrom;
        protected OptionSpec<File> filesFromSpec;
        protected String delim;
        protected OptionSpec nullDelimitedSpec;
        protected OptionSpec<String> inputDelimiterSpec;
        protected File output;
        protected OptionSpec<File> outputFileSpec;
        protected File baseName;
        protected OptionSpec<File> baseFileSpec;
        protected int suffixLength;
        protected OptionSpec<Integer> suffixLengthSpec;
        protected String outputExtension;
        protected OptionSpec<String> outputExtensionSpec;
        protected boolean noIndent;
        protected OptionSpec noIndentSpec;

        protected OptionSpec verboseSpec;
        protected OptionSpec helpSpec;

        protected final OptionParser parser;

        private InputSource inSource;

        protected InputCommand(boolean first, boolean last) {
            if (!first || last) {
                throw new IllegalArgumentException(InputCommand.class + " must be first in chain");
            }
            parser = new OptionParser();
            inputFileSpec = parser.acceptsAll(Flags.INPUT_FILE_ARG, "input; default to stdin if no --files-from, otherwise CWD")
                    .withRequiredArg().ofType(File.class)
                    .describedAs("'-' for stdin");
            filesFromSpec = parser.acceptsAll(Flags.FILES_FROM_ARG, "indirect input")
                    .withRequiredArg().ofType(File.class)
                    .describedAs("'-' for stdin");
            nullDelimitedSpec = parser.acceptsAll(Flags.FROM0_ARG, "indirect input file null-delimited");
            inputDelimiterSpec = parser.acceptsAll(Flags.INPUT_DELIMITER_ARG, "directly specify input delimiter")
                    .withRequiredArg().ofType(String.class).defaultsTo(System.lineSeparator());
            verboseSpec = parser.acceptsAll(Flags.VERBOSE_ARG, "be more verbose");
            helpSpec = parser.acceptsAll(Flags.HELP_ARG, "show help").forHelp();
        }

        @Override
        public void printHelpOn(OutputStream out) {
            try {
                parser.printHelpOn(System.err);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }

        public boolean init(String[] args, CommandType type) {
            return initOptionSet(parser.parse(args), type);
        }
        
        protected boolean initOptionSet(OptionSet options, CommandType type) {
            if (options.has(helpSpec)) {
                return false;
            }
            if (options.has(filesFromSpec)) {
                filesFrom = options.valueOf(filesFromSpec);
            } else if (type == CommandType.JOIN) {
                filesFrom = new File("-");
            }
            if (options.has(nullDelimitedSpec)) {
                delim = Character.toString('\0');
            } else {
                delim = options.valueOf(inputDelimiterSpec);
            }
            if (options.has(inputFileSpec)) {
                input = options.valueOf(inputFileSpec);
            } else {
                if (filesFrom == null) {
                    input = new File("-"); // if no files-from, default to stdin
                } else {
                    input = new File(""); // if files-from, default to CWD
                }
            }
            return true;
        }

        protected static InputSource configureInputSource(InputSource in, File file) throws FileNotFoundException {
            if ("-".equals(file.getPath())) {
                in.setByteStream(System.in);
            } else {
                in.setByteStream(new BufferedInputStream(new FileInputStream(file)));
                in.setSystemId(file.getAbsolutePath());
            }
            return in;
        }

        @Override
        public InputSource getInput() throws FileNotFoundException {
            if (inSource != null) {
                return inSource;
            } else {
                inSource = new InputSource();
                if (filesFrom != null) {
                    configureInputSource(inSource, filesFrom);
                } else if (input != null) {
                    configureInputSource(inSource, input);
                } else {
                    throw new AssertionError();
                }
                return inSource;
            }
        }

        @Override
        public XMLFilter getXMLFilter(String[] args, Command inputBase, CommandType maxType) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public File getInputBase() {
            return input;
        }

        @Override
        public CommandType getCommandType() {
            return CommandType.PASS_THROUGH;
        }

    }

}
