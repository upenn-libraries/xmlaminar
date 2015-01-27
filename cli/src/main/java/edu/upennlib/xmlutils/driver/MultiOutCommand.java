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
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.XMLFilter;

/**
 *
 * @author magibney
 */
public abstract class MultiOutCommand implements Command<InputCommandFactory.InputCommand> {

    protected int recordDepth;
    protected OptionSpec<Integer> recordDepthSpec;
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

    protected InputCommandFactory.InputCommand inputBase;

    protected MultiOutCommand(boolean first, boolean last) {
        this.first = first;
        this.last = last;
        parser = new OptionParser();
        recordDepthSpec = parser.acceptsAll(Flags.DEPTH_ARG, "set record element depth")
                .withRequiredArg().ofType(Integer.class).defaultsTo(1);
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

    protected boolean init(OptionSet options, InputCommandFactory.InputCommand inputBase) {
        this.inputBase = inputBase;
        if (options.has(helpSpec)) {
            return false;
        }
        recordDepth = options.valueOf(recordDepthSpec);
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
                if (inputBase.filesFrom == null) {
                    output = new File("-");
                } else if (baseName != null) {
                    output = new File("");
                } else {
                    throw new IllegalArgumentException("if " + Flags.FILES_FROM_ARG.get(0)
                            + " specified, " + Flags.OUTPUT_BASE_NAME_ARG.get(0) + " or "
                            + Flags.OUTPUT_FILE_ARG.get(0) + " must also be specified");
                }
            }
        }
        return true;
    }

    @Override
    public InputSource getInput() throws FileNotFoundException {
        if (first) {
            return inputBase.getInput();
        } else {
            return null;
        }
    }

}
