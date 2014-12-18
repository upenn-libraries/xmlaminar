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
import java.util.logging.Level;
import java.util.logging.Logger;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.xml.sax.InputSource;

/**
 *
 * @author magibney
 */
public abstract class MultiOutCommand implements Command {

    protected int recordDepth;
    protected OptionSpec<Integer> recordDepthSpec;
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
    
    protected final boolean first;
    protected final boolean last;
    
    private InputSource inSource;
    
    protected MultiOutCommand(String[] args, boolean first, boolean last) {
        this.first = first;
        this.last = last;
        parser = new OptionParser();
        recordDepthSpec = parser.acceptsAll(Flags.DEPTH_ARG, "set record element depth")
                .withRequiredArg().ofType(Integer.class).defaultsTo(1);
        if (first) {
            inputFileSpec = parser.acceptsAll(Flags.INPUT_FILE_ARG, "input; default to stdin if no --files-from, otherwise CWD")
                    .withRequiredArg().ofType(File.class)
                    .describedAs("'-' for stdin");
            filesFromSpec = parser.acceptsAll(Flags.FILES_FROM_ARG, "indirect input")
                    .withRequiredArg().ofType(File.class)
                    .describedAs("'-' for stdin");
            nullDelimitedSpec = parser.acceptsAll(Flags.FROM0_ARG, "indirect input file null-delimited");
            inputDelimiterSpec = parser.acceptsAll(Flags.INPUT_DELIMITER_ARG, "directly specify input delimiter")
                    .withRequiredArg().ofType(String.class).defaultsTo(System.lineSeparator());
        }
        if (last) {
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
        }
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

    protected boolean init(OptionSet options) {
        if (options.has(helpSpec)) {
            return false;
        }
        recordDepth = options.valueOf(recordDepthSpec);
        if (first) {
            if (options.has(filesFromSpec)) {
                filesFrom = options.valueOf(filesFromSpec);
                if (options.has(nullDelimitedSpec)) {
                    delim = Character.toString('\0');
                } else {
                    delim = options.valueOf(inputDelimiterSpec);
                }
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
        }
        if (last) {
            noIndent = options.has(noIndentSpec);
            suffixLength = options.valueOf(suffixLengthSpec);
            outputExtension = options.valueOf(outputExtensionSpec);
            if (options.has(baseFileSpec)) {
                baseName = options.valueOf(baseFileSpec);
            }
            if (options.has(outputFileSpec)) {
                output = options.valueOf(outputFileSpec);
            } else {
                if (filesFrom == null) {
                    output = new File("-");
                } else if (baseName != null) {
                    output = new File("");
                } else {
                    throw new IllegalArgumentException("if "+Flags.FILES_FROM_ARG.get(0)
                            + " specified, " + Flags.OUTPUT_BASE_NAME_ARG.get(0) + " or "
                            + Flags.OUTPUT_FILE_ARG.get(0) + " must also be specified");
                }
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
            } else if (first) {
                inSource = new InputSource();
                if (filesFrom != null) {
                    configureInputSource(inSource, filesFrom);
                } else if (input != null) {
                    configureInputSource(inSource, input);
                } else {
                    throw new AssertionError();
                }
                return inSource;
            } else {
                return null;
            }
        }


}
