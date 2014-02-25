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

import edu.upennlib.xmlutils.SplittingXMLFilter;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Arrays;
import joptsimple.OptionParser;

import static java.util.Arrays.asList;
import java.util.List;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.stream.StreamResult;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 *
 * @author michael
 */
public class Driver {

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
    private static final List<String> SIZE_ARG = asList("s", "output-size");
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
                throw new AssertionError("must implement new command: "+commandType);
        }
        command.configure(parser, specs);
        OptionSet options = parser.parse(commandArgs);
        init(options, specs);
    }

    static class SpecStruct {

        private OptionSpec<Integer> outputSizeSpec;
        private OptionSpec<Integer> suffixSizeSpec;
        private OptionSpec<String> additionalSuffixSpec;
        
        private OptionSpec nullDelimitedSpec;
        private OptionSpec<String> inputDelimiterSpec;
    
        private OptionSpec<InputFileType> inputFileTypeSpec;
    
        private OptionSpec<File> inputFileSpec;
        private OptionSpec<File> outputFileSpec;
        private OptionSpec<Integer> recordDepthSpec;
        private OptionSpec verboseSpec;
        private OptionSpec helpSpec;
    }
    
    private static OptionParser configureSplitParser(OptionParser parser, SpecStruct specs) {
        specs.outputSizeSpec = parser.acceptsAll(SIZE_ARG).withRequiredArg().ofType(Integer.class)
                .describedAs("size (in records) of output files").defaultsTo(100);
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
        
        outputSize = getValue(options, specs.outputSizeSpec, -1);
        suffixSize = getValue(options, specs.suffixSizeSpec, -1);
        additionalSuffix = getValue(options, specs.additionalSuffixSpec, null);
        
        nullDelimited = options.has(specs.nullDelimitedSpec);
        inputDelimiter = getValue(options, specs.inputDelimiterSpec, null);
    
        inputFileType = getValue(options, specs.inputFileTypeSpec, null);
        
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
    
    private int outputSize;
    private int suffixSize;
    private String additionalSuffix;
    
    private boolean nullDelimited;
    private String inputDelimiter;
    
    private InputFileType inputFileType;
    
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

        }

        @Override
        protected OptionParser configure(OptionParser parser, SpecStruct specs) {
            return configureJoinParser(parser, specs);
        }

    }

    private class SplitCommand extends Command {
        @Override
        public void run() {
            InputSource in;
            Reader r = null;
            if ("-".equals(inputFile.getPath())) {
                in = new InputSource(System.in);
            } else {
                try {
                    in = new InputSource(r = new FileReader(inputFile));
                } catch (FileNotFoundException ex) {
                    throw new RuntimeException(ex);
                }
            }
            try {
                SplittingXMLFilter splitter = new SplittingXMLFilter();
                splitter.setChunkSize(outputSize);
                SAXParserFactory spf = SAXParserFactory.newInstance();
                spf.setNamespaceAware(true);
                SAXParser saxParser = spf.newSAXParser();
                splitter.setParent(saxParser.getXMLReader());
                Transformer t = TransformerFactory.newInstance().newTransformer();
                int nextFileNumber = 0;
                String numberFormatString = "%0"+suffixSize+'d';
                File outputDir = outputFile.getParentFile();
                do {
                    File out = new File(outputDir, 
                            outputFile.getName()
                            + String.format(numberFormatString, nextFileNumber++)
                            + additionalSuffix);
                    BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(out));
                    try {
                        t.transform(new SAXSource(splitter, in), new StreamResult(bos));
                    } finally {
                        bos.close();
                    }
                } while (splitter.hasMoreOutput(in));
            } catch (ParserConfigurationException ex) {
                throw new RuntimeException(ex);
            } catch (SAXException ex) {
                throw new RuntimeException(ex);
            } catch (TransformerConfigurationException ex) {
                throw new RuntimeException(ex);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            } catch (TransformerException ex) {
                throw new RuntimeException(ex);
            } finally {
                try {
                    if (r != null) {
                        r.close();
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
