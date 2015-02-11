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

import edu.upennlib.paralleltransformer.JoiningXMLFilter;
import edu.upennlib.paralleltransformer.SerializingXMLFilter;
import edu.upennlib.paralleltransformer.callback.BaseRelativeFileCallback;
import edu.upennlib.paralleltransformer.callback.BaseRelativeIncrementingFileCalback;
import edu.upennlib.paralleltransformer.callback.IncrementingFileCallback;
import edu.upennlib.paralleltransformer.callback.OutputCallback;
import edu.upennlib.paralleltransformer.callback.StaticFileCallback;
import edu.upennlib.paralleltransformer.callback.StdoutCallback;
import edu.upennlib.xmlutils.VolatileXMLFilterImpl;
import static edu.upennlib.xmlutils.driver.InputCommandFactory.InputCommand.trimOptionFlag;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerFactory;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXNotSupportedException;
import org.xml.sax.XMLFilter;
import org.xml.sax.XMLReader;

/**
 *
 * @author magibney
 */
class OutputCommandFactory extends CommandFactory {

    private static final String KEY = "output";

    static {
        registerCommandFactory(new OutputCommandFactory());
    }

    @Override
    public Command newCommand(boolean first, boolean last) {
        return new OutputCommand(first, last);
    }

    @Override
    public String getKey() {
        return KEY;
    }

    @Override
    public CommandFactory getConfiguringXMLFilter(boolean first, InitCommand inputBase, CommandType maxType) {
        return null;
    }

    public static class OutputCommand implements Command<InputCommandFactory.InputCommand> {

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
        protected boolean gzipOutput;
        protected OptionSpec gzipOutputSpec;

        protected OptionSpec verboseSpec;
        protected OptionSpec helpSpec;

        protected final OptionParser parser;

        private InputCommandFactory.InputCommand inputBase;
        private XMLFilter ret;
        private final boolean first;
        private final boolean last;

        protected OutputCommand(boolean first, boolean last) {
            this.first = first;
            this.last = last;
            parser = new OptionParser();
            outputFileSpec = parser.acceptsAll(Flags.OUTPUT_FILE_ARG, "output")
                    .withRequiredArg().ofType(File.class)
                    .describedAs("'-' for stdout");
            baseFileSpec = parser.acceptsAll(Flags.OUTPUT_BASE_NAME_ARG, "output base name")
                    .withRequiredArg().ofType(File.class)
                    .describedAs("'-' for stdout");
            suffixLengthSpec = parser.acceptsAll(Flags.SUFFIX_LENGTH_ARG, "size of incremented suffix")
                    .withRequiredArg().ofType(Integer.class).defaultsTo(5);
            outputExtensionSpec = parser.acceptsAll(Flags.OUTPUT_EXTENSION_ARG, "optional additional suffix")
                    .withRequiredArg().ofType(String.class);
            noIndentSpec = parser.acceptsAll(Flags.NO_INDENT_ARG, "prevent default indenting of output");
            gzipOutputSpec = parser.acceptsAll(Flags.GZIP_OUTPUT_ARG, "gzip output");

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

        protected boolean init(OptionSet options, InputCommandFactory.InputCommand inputBase) {
            if (options.has(helpSpec)) {
                return false;
            }
            noIndent = options.has(noIndentSpec);
            gzipOutput = options.has(gzipOutputSpec);
            suffixLength = options.valueOf(suffixLengthSpec);
            outputExtension = options.valueOf(outputExtensionSpec);
            if (options.has(baseFileSpec)) {
                baseName = options.valueOf(baseFileSpec);
            }
            if (options.has(outputFileSpec)) {
                output = options.valueOf(outputFileSpec);
            } else if (output == null) {
                if (inputBase.filesFrom == null) {
                    output = new File("-");
                } else if (baseName != null) {
                    output = new File("");
                } else {
                    output = new File("-");
//                    throw new IllegalArgumentException("if " + Flags.FILES_FROM_ARG.get(0)
//                            + " specified, " + Flags.OUTPUT_BASE_NAME_ARG.get(0) + " or "
//                            + Flags.OUTPUT_FILE_ARG.get(0) + " must also be specified");
                }
            }
            return true;
        }

        @Override
        public InputSource getInput() throws FileNotFoundException {
            return null;
        }

        private String[] parseMainOut(String[] args) {
            if (args == null || args.length < 1) {
                return args;
            } else {
                String mainOutSpec = args[0];
                if (parser.recognizedOptions().keySet().contains(trimOptionFlag(mainOutSpec))) {
                    return args;
                }
                output = new File(mainOutSpec);
                return Arrays.copyOfRange(args, 1, args.length);
            }
        }
        
        @Override
        public XMLFilter getXMLFilter(ArgFactory arf, InputCommandFactory.InputCommand inputBase, CommandType maxType) {
            if (ret != null) {
                return ret;
            }
            this.inputBase = inputBase;
            if (!init(parser.parse(parseMainOut(arf.getArgs(parser.recognizedOptions().keySet()))), inputBase)) {
                return null;
            }
            if (first || !last) {
                throw new IllegalArgumentException(KEY + " command must be last, and must not be first");
            }
            String inBaseSystemId = inputBase.input.getSystemId();
            File inBaseFile = inBaseSystemId == null ? null : new File(inBaseSystemId);
            ret = new OutputXMLFilter(inBaseFile, output, baseName, suffixLength, outputExtension, noIndent, gzipOutput);
            return ret;
        }

        @Override
        public File getInputBase() {
            return inputBase.getInputBase();
        }

        @Override
        public CommandType getCommandType() {
            return CommandType.PASS_THROUGH;
        }

        @Override
        public boolean handlesOutput() {
            return true;
        }

        @Override
        public InputCommandFactory.InputCommand inputHandler() {
            return inputBase;
        }

    }

    private static class OutputXMLFilter extends VolatileXMLFilterImpl {

        private final File inputBase;
        private final File output;
        private final File baseName;
        private final int suffixLength;
        private final String outputExtension;
        private final boolean noIndent;
        private final boolean gzipOutput;

        private OutputXMLFilter(File inputBase, File output, File baseName, int suffixLength, String outputExtension, boolean noIndent, boolean gzipOutput) {
            this(inputBase, output, baseName, suffixLength, outputExtension, noIndent, null, gzipOutput);
        }

        public OutputXMLFilter(File inputBase, File output, File baseName, int suffixLength, String outputExtension, boolean noIndent, XMLReader parent, boolean gzipOutput) {
            super(parent);
            this.inputBase = inputBase;
            this.output = output;
            this.baseName = baseName;
            this.suffixLength = suffixLength;
            this.outputExtension = outputExtension;
            this.noIndent = noIndent;
            this.gzipOutput = gzipOutput;
        }

        @Override
        public void parse(String systemId) throws SAXException, IOException {
            if (setupParse(null)) {
                super.parse(systemId);
            }
        }

        @Override
        public void parse(InputSource input) throws SAXException, IOException {
            if (setupParse(input)) {
                super.parse(input);
            }
        }
        
        private static boolean ASSUME_GROUP_BY_SYSTEMID = true;
        
        private static boolean groupBySystemId(XMLReader reader) {
            try {
                return reader.getFeature(JoiningXMLFilter.GROUP_BY_SYSTEMID_FEATURE_NAME);
            } catch (SAXNotRecognizedException ex) {
                return ASSUME_GROUP_BY_SYSTEMID;
            } catch (SAXNotSupportedException ex) {
                return ASSUME_GROUP_BY_SYSTEMID;
            }
        }

        private boolean setupParse(InputSource inSource) throws SAXException, IOException {
            XMLReader parent = getParent();
            OutputCallback callbackParent;
            if (parent instanceof OutputCallback
                    && (callbackParent = (OutputCallback) parent).allowOutputCallback()) {
                Transformer t;
                try {
                    t = TransformerFactory.newInstance().newTransformer();
                } catch (TransformerConfigurationException ex) {
                    throw new RuntimeException(ex);
                }
                XMLFilter outputFilter = noIndent ? null : new OutputTransformerConfigurer(Collections.singletonMap("indent", "yes"));
                if (baseName != null) {
                    File resolvedBase;
                    if (output.isDirectory()) {
                        resolvedBase = new File(output.toURI().resolve(baseName.toURI()));
                    } else {
                        resolvedBase = baseName;
                    }
                    callbackParent.setOutputCallback(new IncrementingFileCallback(0,
                            t, suffixLength, resolvedBase, outputExtension, outputFilter, gzipOutput));
                } else if ("-".equals(output.getPath())) {
                    callbackParent.setOutputCallback(new StdoutCallback(t, outputFilter, gzipOutput));
                } else if (!output.isDirectory()) {
                    callbackParent.setOutputCallback(new StaticFileCallback(t, output, outputFilter, gzipOutput));
                } else {
                    if (groupBySystemId(parent)) {
                        callbackParent.setOutputCallback(new BaseRelativeFileCallback(inputBase, output, t, gzipOutput));
                    } else {
                        callbackParent.setOutputCallback(new BaseRelativeIncrementingFileCalback(inputBase, output, t, outputExtension, outputExtension != null, suffixLength, outputFilter, gzipOutput));
                    }
                }
                return true;
            } else {
                SerializingXMLFilter serializer = new SerializingXMLFilter(output);
                serializer.setParent(noIndent ? parent : new OutputTransformerConfigurer(parent, Collections.singletonMap("indent", "yes")));
                serializer.parse(inSource);
                return false;
            }
        }

    }
}
