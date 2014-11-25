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
import edu.upennlib.paralleltransformer.LevelSplittingXMLFilter;
import edu.upennlib.paralleltransformer.QueueSourceXMLFilter;
import edu.upennlib.paralleltransformer.SerializingXMLFilter;
import edu.upennlib.paralleltransformer.callback.BaseRelativeFileCallback;
import edu.upennlib.paralleltransformer.callback.IncrementingFileCallback;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.sax.SAXTransformerFactory;
import javax.xml.transform.sax.TransformerHandler;
import javax.xml.transform.stream.StreamResult;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.xml.sax.InputSource;
import org.xml.sax.XMLFilter;
import org.xml.sax.helpers.XMLFilterImpl;

/**
 *
 * @author magibney
 */
public class JoinCommand implements Command {

    protected File filesFrom;
    protected OptionSpec<File> filesFromSpec;
    protected String delim;
    protected OptionSpec nullDelimitedSpec;
    protected OptionSpec<String> inputDelimiterSpec;
    protected File output;
    protected OptionSpec<File> outputFileSpec;

    protected OptionSpec verboseSpec;
    protected OptionSpec helpSpec;

    protected final OptionParser parser;
    
    private final String[] args;
    protected final boolean first;
    protected final boolean last;
    
    private InputSource inSource;
    
    protected JoinCommand(String[] args, boolean first, boolean last) {
        this.args = args;
        this.first = first;
        this.last = last;
        parser = new OptionParser();
        if (first) {
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
        if (first) {
            if (options.has(nullDelimitedSpec)) {
                delim = Character.toString('\0');
            } else {
                delim = options.valueOf(inputDelimiterSpec);
            }
            if (options.has(filesFromSpec)) {
                filesFrom = options.valueOf(filesFromSpec);
            } else {
                filesFrom = new File("-");
            }
        }
        if (last) {
            if (options.has(outputFileSpec)) {
                output = options.valueOf(outputFileSpec);
            } else {
                output = new File("-");
            }
        }
        return true;
    }

    protected static void configureInputSource(InputSource in, File file) throws FileNotFoundException {
        if ("-".equals(file.getPath())) {
            in.setByteStream(System.in);
        } else {
            in.setByteStream(new BufferedInputStream(new FileInputStream(file)));
            in.setSystemId(file.getAbsolutePath());
        }
    }

    @Override
    public InputSource getInput() throws FileNotFoundException {
        if (inSource != null) {
            return inSource;
        } else if (first) {
            inSource = new InputSource();
            if (filesFrom != null) {
                configureInputSource(inSource, filesFrom);
            } else {
                throw new AssertionError();
            }
            return inSource;
        } else {
            return null;
        }
    }

    @Override
    public XMLFilter getXMLFilter(File inputBase) {
        if (!init(parser.parse(args))) {
            return null;
        }
        JoiningXMLFilter joiner = new JoiningXMLFilter();
        if (filesFrom != null) {
            joiner.setInputType(QueueSourceXMLFilter.InputType.indirect);
        }
        if (!last) {
            return joiner;
        } else {
            SerializingXMLFilter serializer = new SerializingXMLFilter(output);
            serializer.setParent(joiner);
            return serializer;
        }
    }

    @Override
    public CommandType getCommandType() {
        return CommandType.JOIN;
    }

    @Override
    public File inputBase() {
        return null;
    }

}
