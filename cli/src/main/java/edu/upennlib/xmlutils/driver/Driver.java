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

import edu.upennlib.paralleltransformer.callback.IncrementingFileCallback;
import edu.upennlib.paralleltransformer.JoiningXMLFilter;
import edu.upennlib.paralleltransformer.LevelSplittingXMLFilter;
import edu.upennlib.paralleltransformer.QueueSourceXMLFilter;
import edu.upennlib.paralleltransformer.SplittingXMLFilter;
import edu.upennlib.paralleltransformer.TXMLFilter;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.util.Arrays;
import joptsimple.OptionParser;

import static java.util.Arrays.asList;
import java.util.List;
import java.util.concurrent.Executors;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 *
 * @author michael
 */
public class Driver {

    private static final Logger logger = LoggerFactory.getLogger(Driver.class);
    
    /*
    BASE OPTIONS
    */
    private static final List<String> INPUT_FILE_ARG = asList("i", "input-file");
    private static final List<String> OUTPUT_FILE_ARG = asList("o", "output-basename");
    private static final List<String> DEPTH_ARG = asList("r", "record-depth");
    private static final List<String> VERBOSE_ARG = asList("v", "verbose");
    private static final List<String> HELP_ARG = asList("h", "help");

    /*
    SPLIT OPTIONS
    */
    private static final List<String> SIZE_ARG = asList("s", "chunk-size");
    private static final List<String> SUFFIX_LENGTH_ARG = asList("l", "suffix-length");
    private static final String ADDITIONAL_SUFFIX_ARG = "additional-suffix";

    /*
    JOIN OPTIONS
    */
    private static final List<String> FROM0_ARG = asList("0", "from0");
    private static final List<String> INPUT_DELIMITER_ARG = asList("d", "input-delimiter");
    
    /*
    PROCESS OPTIONS
    */
    private static final List<String> INPUT_FILE_TYPE_ARG = asList("t", "input-file-type");
    private static final List<String> XSL_FILE_ARG = asList("x", "xsl");
    
    private final OptionParser parser;
    private final Command command;
    private final Command.Type commandType;

    public Driver(Command.Type commandType, String[] commandArgs) {
        this.commandType = commandType;
        SpecStruct specs = new SpecStruct();
        parser = configureBaseParser(new OptionParser(), specs);
        switch (commandType) {
            case split:
                command = new SplitCommand();
                break;
            case join:
                command = new JoinCommand();
                break;
            case process:
                command = new ProcessCommand();
                break;
            case help:
                command = new HelpCommand();
                break;
            default:
                throw new AssertionError("command not implemented: "+commandType);
        }
        command.configure(parser, specs);
        OptionSet options = parser.parse(commandArgs);
        init(options, specs);
    }

    static class SpecStruct {

        private OptionSpec<Integer> chunkSizeSpec;
        private OptionSpec<Integer> suffixSizeSpec;
        private OptionSpec<String> additionalSuffixSpec;
        
        private OptionSpec nullDelimitedSpec;
        private OptionSpec<String> inputDelimiterSpec;
    
        private OptionSpec<InputFileType> inputFileTypeSpec;
        private OptionSpec<File> xslFileSpec;
    
        private OptionSpec<File> inputFileSpec;
        private OptionSpec<File> outputFileSpec;
        private OptionSpec<Integer> recordDepthSpec;
        private OptionSpec verboseSpec;
        private OptionSpec helpSpec;
    }
    
    private static OptionParser configureSplitParser(OptionParser parser, SpecStruct specs) {
        specs.chunkSizeSpec = parser.acceptsAll(SIZE_ARG).withRequiredArg().ofType(Integer.class)
                .describedAs("size (in records) of output files or processing chunks for xsl transforms").defaultsTo(100);
        specs.suffixSizeSpec = parser.acceptsAll(SUFFIX_LENGTH_ARG, "size of incremented suffix")
                .withRequiredArg().ofType(Integer.class).defaultsTo(5);
        specs.additionalSuffixSpec = parser.accepts(ADDITIONAL_SUFFIX_ARG, "optional additional suffix")
                .withRequiredArg().ofType(String.class).defaultsTo("");
        return parser;
    }

    private static OptionParser configureJoinParser(OptionParser parser, SpecStruct specs) {
        specs.nullDelimitedSpec = parser.acceptsAll(FROM0_ARG, "input reference file null-delimited");
        specs.inputDelimiterSpec = parser.acceptsAll(INPUT_DELIMITER_ARG, "input delimiter override")
                .withRequiredArg().ofType(String.class).defaultsTo(System.lineSeparator());
        return parser;
    }

    private static OptionParser configureProcessParser(OptionParser parser, SpecStruct specs) {
        configureSplitParser(parser, specs);
        configureJoinParser(parser, specs);
        specs.inputFileTypeSpec = parser.acceptsAll(INPUT_FILE_TYPE_ARG, "input file type").withRequiredArg()
                .ofType(InputFileType.class).describedAs(asList(InputFileType.values()).toString()).defaultsTo(InputFileType.direct);
        specs.xslFileSpec = parser.acceptsAll(XSL_FILE_ARG, "xsl file").withRequiredArg()
                .ofType(File.class);
               
        return parser;
    }

    private static enum InputFileType { direct, reference }
    
    private OptionParser configureBaseParser(OptionParser parser, SpecStruct specs) {
        specs.inputFileSpec = parser.acceptsAll(INPUT_FILE_ARG)
                .withRequiredArg().ofType(File.class)
                .describedAs("input file (for split) or path to delimited list of input files (for combine); '-' for stdin");
        specs.outputFileSpec = parser.acceptsAll(OUTPUT_FILE_ARG)
                .withRequiredArg().ofType(File.class)
                .describedAs("output filename (for combine) or filename prefix (for split)");
        specs.recordDepthSpec = parser.acceptsAll(DEPTH_ARG, "set record element depth")
                .withRequiredArg().ofType(Integer.class).defaultsTo(1);
        specs.verboseSpec = parser.acceptsAll(VERBOSE_ARG, "be more verbose");
        specs.helpSpec = parser.acceptsAll(HELP_ARG, "show help").forHelp();
        return parser;
    }
    
    private void init(OptionSet options, SpecStruct specs) {
        inputFile = getValue(options, specs.inputFileSpec, null);
        outputFile = getValue(options, specs.outputFileSpec, null);
        recordDepth = options.valueOf(specs.recordDepthSpec);
        verbose = options.has(specs.verboseSpec);
        help = options.has(specs.helpSpec);
        
        chunkSize = getValue(options, specs.chunkSizeSpec, -1);
        suffixSize = getValue(options, specs.suffixSizeSpec, -1);
        additionalSuffix = getValue(options, specs.additionalSuffixSpec, null);
        
        nullDelimited = options.has(specs.nullDelimitedSpec);
        if (specs.inputDelimiterSpec != null && options.has(specs.inputDelimiterSpec)) {
            inputDelimiter = options.valueOf(specs.inputDelimiterSpec);
        } else {
            inputDelimiter = nullDelimited ? Character.toString('\0') : System.lineSeparator();
        }
    
        inputFileType = getValue(options, specs.inputFileTypeSpec, null);
        xsl = getValue(options, specs.xslFileSpec, null);
        
    }
    
    private static <V> V getValue(OptionSet options, OptionSpec<V> spec, V defaultValue) {
        if (spec == null) {
            return defaultValue;
        } else {
            return options.valueOf(spec);
        }
    }
    
    public static void main(String[] args) throws IOException {

        String commandString = "[null]";
        Command.Type commandType;
        try {
            commandType = Command.Type.valueOf(commandString = args[0].toLowerCase());
        } catch (Exception ex) {
            System.err.println("illegal command specification: "+commandString+"; command must be one of: ");
            System.err.println("  "+asList(Command.Type.values()));
            Thread.currentThread().setUncaughtExceptionHandler(new QuietUEH());
            throw new RuntimeException(ex);
        }
        
        Driver d = new Driver(commandType, Arrays.copyOfRange(args, 1, args.length));
        d.command.run();
        
    }

    private File inputFile;
    private File outputFile;
    private int recordDepth;
    private boolean verbose;
    private boolean help;
    
    private int chunkSize;
    private int suffixSize;
    private String additionalSuffix;
    
    private boolean nullDelimited;
    private String inputDelimiter;
    
    private InputFileType inputFileType;
    private File xsl;
    
    private class HelpCommand extends Command {

        @Override
        public void run() {
            try {
                parser.printHelpOn(System.out);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }

        @Override
        protected OptionParser configure(OptionParser parser, SpecStruct specs) {
            return parser;
        }
        
    }
    
    private class JoinCommand extends Command {

        @Override
        public void run() {
            Reader r;
            InputSource source;
            OutputStream out = null;
            if (inputFile == null || "-".equals(inputFile.getPath())) {
                r = new InputStreamReader(System.in);
                source = new InputSource(r);
            } else {
                try {
                    r = new FileReader(inputFile);
                } catch (FileNotFoundException ex) {
                    throw new RuntimeException(ex);
                }
                source = new InputSource(r);
                source.setSystemId(inputFile.getAbsolutePath());
            }
            try {
                JoiningXMLFilter joiner = new JoiningXMLFilter();
                SAXParserFactory spf = SAXParserFactory.newInstance();
                spf.setNamespaceAware(true);
                joiner.setParent(spf.newSAXParser().getXMLReader());
                StreamResult res = new StreamResult();
                if (outputFile == null || "-".equals(outputFile.getPath())) {
                    out = System.out;
                    res.setOutputStream(out);
                } else {
                    out = new BufferedOutputStream(new FileOutputStream(outputFile));
                    res.setOutputStream(out);
                    res.setSystemId(outputFile);
                }
                Transformer t = TransformerFactory.newInstance().newTransformer();
                t.transform(new SAXSource(joiner, source), res);
            } catch (TransformerConfigurationException ex) {
                throw new RuntimeException(ex);
            } catch (TransformerException ex) {
                throw new RuntimeException(ex);
            } catch (FileNotFoundException ex) {
                throw new RuntimeException(ex);
            } catch (ParserConfigurationException ex) {
                throw new RuntimeException(ex);
            } catch (SAXException ex) {
                throw new RuntimeException(ex);
            } finally {
                try {
                    r.close();
                    if (out != null) {
                        out.close();
                    }
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            }
        }

        @Override
        protected OptionParser configure(OptionParser parser, SpecStruct specs) {
            return configureJoinParser(parser, specs);
        }

    }

    private class SplitCommand extends Command {
        @Override
        public void run() {
            InputSource source;
            InputStream in = null;
            if ("-".equals(inputFile.getPath())) {
                source = new InputSource(System.in);
            } else {
                try {
                    source = new InputSource(in = new FileInputStream(inputFile));
                } catch (FileNotFoundException ex) {
                    throw new RuntimeException(ex);
                }
            }
            try {
                LevelSplittingXMLFilter sxf = new LevelSplittingXMLFilter();
                sxf.setChunkSize(chunkSize);
                SAXParserFactory spf = SAXParserFactory.newInstance();
                spf.setNamespaceAware(true);
                SAXParser saxParser = spf.newSAXParser();
                sxf.setParent(saxParser.getXMLReader());
                Transformer t = TransformerFactory.newInstance().newTransformer();
                sxf.setOutputCallback(new IncrementingFileCallback(0, t, suffixSize, outputFile, additionalSuffix));
                sxf.setExecutor(Executors.newCachedThreadPool());
                try {
                    sxf.parse(source);
                } finally {
                    sxf.getExecutor().shutdown();
                }
            } catch (ParserConfigurationException ex) {
                throw new RuntimeException(ex);
            } catch (SAXException ex) {
                throw new RuntimeException(ex);
            } catch (TransformerConfigurationException ex) {
                throw new RuntimeException(ex);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            } finally {
                try {
                    if (in != null) {
                        in.close();
                    }
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            }
        }
        
        @Override
        protected OptionParser configure(OptionParser parser, SpecStruct specs) {
            return configureSplitParser(parser, specs);
        }
        
    }
    
    private class ProcessCommand extends Command {
        @Override
        public void run() {
            Reader r;
            InputSource source;
            OutputStream out = null;
            if (inputFile == null || "-".equals(inputFile.getPath())) {
                r = new InputStreamReader(System.in);
                source = new InputSource(r);
            } else {
                try {
                    r = new FileReader(inputFile);
                } catch (FileNotFoundException ex) {
                    throw new RuntimeException(ex);
                }
                source = new InputSource(r);
                source.setSystemId(inputFile.getAbsolutePath());
            }
            try {
                LevelSplittingXMLFilter sxf = new LevelSplittingXMLFilter(recordDepth, chunkSize);
                QueueSourceXMLFilter.InputType inputType;
                switch (inputFileType) {
                    case direct:
                        inputType = QueueSourceXMLFilter.InputType.direct;
                        break;
                    case reference:
                        inputType = QueueSourceXMLFilter.InputType.indirect;
                        break;
                    default:
                        throw new AssertionError();
                }
                sxf.setInputType(inputType);
                TXMLFilter txf = new TXMLFilter(new StreamSource(xsl));
                JoiningXMLFilter joiner = new JoiningXMLFilter();
                txf.setParent(sxf);
                joiner.setParent(txf);
                StreamResult res = new StreamResult();
                if (outputFile == null || "-".equals(outputFile.getPath())) {
                    out = System.out;
                    res.setOutputStream(out);
                } else {
                    out = new BufferedOutputStream(new FileOutputStream(outputFile));
                    res.setOutputStream(out);
                    res.setSystemId(outputFile);
                }
                Transformer t = TransformerFactory.newInstance("net.sf.saxon.TransformerFactoryImpl", null).newTransformer();
                txf.configureOutputTransformer(t);
                t.transform(new SAXSource(joiner, source), res);
            } catch (TransformerConfigurationException ex) {
                throw new RuntimeException(ex);
            } catch (TransformerException ex) {
                throw new RuntimeException(ex);
            } catch (FileNotFoundException ex) {
                throw new RuntimeException(ex);
            } finally {
                try {
                    r.close();
                    if (out != null) {
                        out.flush();
                    }
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            }
        }

        @Override
        protected OptionParser configure(OptionParser parser, SpecStruct specs) {
            return configureProcessParser(parser, specs);
        }
        
    }
    
    private static class QuietUEH implements Thread.UncaughtExceptionHandler {

        @Override
        public void uncaughtException(Thread t, Throwable e) {
            System.err.println(getRootCause(e).getMessage());
            System.exit(1);
        }
        
    }
    
    private static Throwable getRootCause(Throwable t) {
        Throwable cause;
        if ((cause = t.getCause()) == null) {
            return t;
        } else {
            return getRootCause(cause);
        }
    }
    
}
