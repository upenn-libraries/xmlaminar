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
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.regex.Matcher;
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

        protected InputSource input;
        protected OptionSpec<File> inputFileSpec;
        protected InputSource filesFrom;
        protected OptionSpec<File> filesFromSpec;
        protected String delim;
        protected OptionSpec nullDelimitedSpec;
        protected OptionSpec<String> inputDelimiterSpec;

        protected OptionSpec verboseSpec;
        protected OptionSpec helpSpec;

        protected final OptionParser parser;

        private static final InputSource UNINITIALIZED_INPUT_SOURCE = new InputSource();
        private InputSource inSource = UNINITIALIZED_INPUT_SOURCE;

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

        public boolean init(String[] args, CommandType type) throws IOException {
            return initOptionSet(parser.parse(parseMainIn(args)), type);
        }
        
        private static final Pattern TRIM_OPTION_FLAG = Pattern.compile("^--?(?=[^-])");
        
        private static String trimOptionFlag(String flag) {
            Matcher m = TRIM_OPTION_FLAG.matcher(flag);
            if (!m.find()) {
                return null;
            } else {
                return flag.substring(m.end());
            }
        }
        
        private String[] parseMainIn(String[] args) throws FileNotFoundException, IOException {
            String mainInSpec;
            if (args == null || args.length < 1) {
                mainInSpec = "-";
            } else {
                mainInSpec = args[0];
                if (parser.recognizedOptions().keySet().contains(trimOptionFlag(mainInSpec))) {
                    return args;
                }
                args = Arrays.copyOfRange(args, 1, args.length);
            }
            File backingFile = null;
            InputStream backing = null;
            if ("-".equals(mainInSpec)) {
                backing = System.in;
            } else if ((backingFile = new File(mainInSpec)).isFile()) {
                backing = new BufferedInputStream(new FileInputStream(backingFile));
            }
            if (backing == null) {
                if (backingFile != null) {
                    input = new InputSource(backingFile.getAbsolutePath());
                }
            } else {
                final int firstByte = backing.read();
                InputSource mainIn = new InputSource(new PrependInputStream(backing, firstByte));
                if (backingFile != null) {
                    mainIn.setSystemId(backingFile.getAbsolutePath());
                }
                if (firstByte == '<') {
                    input = mainIn;
                } else {
                    filesFrom = mainIn;
                }
            }
            return args;
        }
        
        protected boolean initOptionSet(OptionSet options, CommandType type) throws FileNotFoundException, IOException {
            if (options.has(helpSpec)) {
                return false;
            }
            File filesFromFile = null;
            if (options.has(filesFromSpec)) {
                filesFromFile = options.valueOf(filesFromSpec);
            } else if (filesFrom == null && type == CommandType.JOIN) {
                filesFromFile = new File("-");
            }
            if (filesFromFile != null) {
                filesFrom = conditionallyConfigureInputSource(filesFrom, new InputSource(), filesFromFile);
            }
            if (filesFrom != null) {
                if (options.has(nullDelimitedSpec)) {
                    delim = Character.toString('\0');
                } else {
                    delim = options.valueOf(inputDelimiterSpec);
                }
            }
            File inputFile = null;
            if (options.has(inputFileSpec)) {
                inputFile = options.valueOf(inputFileSpec);
            } else {
                if (input == null) {
                    if (filesFrom == null) {
                        inputFile = new File("-"); // if no files-from, default to stdin
                    } else {
                        inputFile = new File(""); // if files-from, default to CWD
                    }
                }
            }
            if (inputFile != null) {
                input = conditionallyConfigureInputSource(input, new InputSource(), inputFile);
            }
            return true;
        }
        
        private static class PrependInputStream extends FilterInputStream {

            public PrependInputStream(InputStream in, int prepend) {
                super(null);
                this.in = new SelfEffacingInputStream(prepend, this, in);
            }
            
            void setBacking(InputStream backing) {
                in = backing;
            }
            
        }
        
        private static class SelfEffacingInputStream extends ByteArrayInputStream {

            private final PrependInputStream parent;
            private final InputStream successor;
            
            public SelfEffacingInputStream(int prepend, PrependInputStream parent, InputStream successor) {
                super(new byte[] {(byte)prepend});
                this.parent = parent;
                this.successor = successor;
            }

            @Override
            public synchronized int read(byte[] b, int off, int len) {
                int ret = super.read(b, off, len);
                parent.setBacking(successor);
                return ret;
            }

            @Override
            public synchronized int read() {
                int ret = super.read();
                parent.setBacking(successor);
                return ret;
            }

            @Override
            public void close() throws IOException {
                super.close();
                successor.close();
            }
            
        }

        public static InputSource configureInputSource(InputSource in, File file) throws FileNotFoundException {
            if ("-".equals(file.getPath())) {
                in.setByteStream(System.in);
            } else {
                if (file.isFile()) {
                    in.setByteStream(new BufferedInputStream(new FileInputStream(file)));
                }
                in.setSystemId(file.getAbsolutePath());
            }
            return in;
        }

        public static InputSource conditionallyConfigureInputSource(InputSource existing, InputSource in, File file) throws FileNotFoundException, IOException {
            if (existing == null) {
                return configureInputSource(in, file);
            } else if ("-".equals(file.getPath())) {
                if (existing.getSystemId() == null) {
                    return existing;
                }
                in.setByteStream(System.in);
            } else {
                if (file.getAbsolutePath().equals(existing.getSystemId())) {
                    return existing;
                } else {
                    InputStream existingIn = existing.getByteStream();
                    if (existingIn != null) {
                        existingIn.close();
                    }
                }
                if (file.isFile()) {
                    in.setByteStream(new BufferedInputStream(new FileInputStream(file)));
                }
                in.setSystemId(file.getAbsolutePath());
            }
            return in;
        }

        @Override
        public InputSource getInput() throws FileNotFoundException {
            if (inSource != UNINITIALIZED_INPUT_SOURCE) {
                return inSource;
            } else {
                inSource = new InputSource();
                if (filesFrom != null) {
                    inSource = filesFrom;
                } else if (input != null) {
                    inSource = input;
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
            if (input == null) {
                return null;
            } else {
                String inputSystemId;
                File inputBase;
                if ((inputSystemId = input.getSystemId()) == null) {
                    return null;
                } else if (!(inputBase = new File(inputSystemId)).isDirectory()) {
                    return null;
                } else {
                    return inputBase;
                }
            }
        }

        @Override
        public CommandType getCommandType() {
            return CommandType.PASS_THROUGH;
        }

    }

}
