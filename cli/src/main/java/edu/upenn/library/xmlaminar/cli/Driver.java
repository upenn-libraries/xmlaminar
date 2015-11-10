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

package edu.upenn.library.xmlaminar.cli;

import edu.upenn.library.xmlaminar.SAXProperties;
import edu.upenn.library.xmlaminar.dbxml.DataSourceFactory;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.sax.SAXSource;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.PatternLayout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXNotSupportedException;
import org.xml.sax.XMLFilter;
import org.xml.sax.XMLReader;

/**
 *
 * @author michael
 */
public class Driver {

    public static final String CONFIG_NAMESPACE_URI = "http://library.upenn.edu/xml-utils/config";
    private static final Logger logger = LoggerFactory.getLogger(Driver.class);
    
    static {
        try {
            // Force initialization of known Command types.
            Class.forName(SplitCommandFactory.class.getCanonicalName());
            Class.forName(ProcessCommandFactory.class.getCanonicalName());
            Class.forName(JoinCommandFactory.class.getCanonicalName());
            Class.forName(TeeCommandFactory.class.getCanonicalName());
            Class.forName(RSXMLReaderCommandFactory.class.getCanonicalName());
            Class.forName(MARCRSXMLReaderCommandFactory.class.getCanonicalName());
            Class.forName(ConfigCommandFactory.class.getCanonicalName());
            Class.forName(PipelineCommandFactory.class.getCanonicalName());
            Class.forName(IntegrateCommandFactory.class.getCanonicalName());
        } catch (ClassNotFoundException ex) {
            throw new RuntimeException(ex);
        }
        
    }
    
    public static class StaticArgFactory implements Command.ArgFactory {

        private final String[] args;
        
        public StaticArgFactory(String[] args) {
            this.args = args;
        }
        
        @Override
        public String[] getArgs(Set<String> recognizedOptions) {
            return args;
        }
        
    }
    
    public static CommandType updateType(CommandType current, CommandType next) {
        return (next.compareTo(current) > 0 ? next : current);
    }
    
    public static class XMLFilterSource<T extends Command & InitCommand> extends SAXSource {

        private XMLFilter filter;
        private File inputBase;
        private CommandType commandType;
        private final boolean handlesOutput;
        private final T inputHandler;

        public XMLFilterSource(XMLReader reader, InputSource inputSource, File inputBase, CommandType commandType, boolean handlesOutput, T inputHandler) {
            this(reader, inputSource, handlesOutput, inputHandler);
            this.inputBase = inputBase;
            this.commandType = commandType;
        }
        
        public XMLFilterSource(XMLReader reader, InputSource inputSource, boolean handlesOutput, T inputHandler) {
            super(reader, inputSource);
            filter = readerToFilter(reader);
            this.handlesOutput = handlesOutput;
            this.inputHandler = inputHandler;
        }
        
        public boolean handlesOutput() {
            return handlesOutput;
        }

        public T inputHandler() {
            return inputHandler;
        }

        @Override
        public XMLFilter getXMLReader() {
            return filter;
        }
        
        @Override
        public void setXMLReader(XMLReader reader) {
            filter = readerToFilter(reader);
            super.setXMLReader(reader);
        }
        
        public void setXMLFilter(XMLFilter filter) {
            this.filter = filter;
            super.setXMLReader(filter);
        }
        
        public File getInputBase() {
            return inputBase;
        }
        
        public CommandType getCommandType() {
            return commandType;
        }
    
        private static XMLFilter readerToFilter(XMLReader reader) {
            if (reader instanceof XMLFilter) {
                return (XMLFilter) reader;
            } else {
                throw new IllegalArgumentException("not instanceof "+XMLFilter.class.getSimpleName()+": "+reader);
            }
        }

    }
    
    private static final InputCommandFactory icf = new InputCommandFactory();
    private static final OutputCommandFactory ocf = new OutputCommandFactory();
    
    public static XMLFilterSource chainCommands(boolean first, InitCommand init, Iterator<Map.Entry<CommandFactory, String[]>> iter, final boolean last) throws FileNotFoundException, IOException {
        XMLFilter previous;
        InputCommandFactory.InputCommand inputCommand = (InputCommandFactory.InputCommand) init;
        InputSource in;
        File inputBase;
        CommandType maxType = null;
        if (iter.hasNext()) {
            Map.Entry<CommandFactory, String[]> commandEntry = iter.next();
            CommandFactory cf = commandEntry.getKey();
            if (inputCommand == null) {
                String[] inputArgs;
                if (cf instanceof InputCommandFactory) {
                    inputCommand = (InputCommandFactory.InputCommand) icf.newCommand(true, false);
                    inputArgs = commandEntry.getValue();
                    if (!iter.hasNext()) {
                        inputCommand.printHelpOn(System.err);
                        return null;
                    }
                    commandEntry = iter.next();
                    cf = commandEntry.getKey();
                } else {
                    inputCommand = (InputCommandFactory.InputCommand) icf.newCommand(true, false, false);
                    inputArgs = new String[0];
                }
                inputCommand.setInputArgs(inputArgs);
            } else if (!inputCommand.isExplicit() && cf instanceof InputCommandFactory) {
                String[] inputArgs = commandEntry.getValue();
                if (!iter.hasNext()) {
                    inputCommand.printHelpOn(System.err);
                    return null;
                }
                commandEntry = iter.next();
                cf = commandEntry.getKey();
                inputCommand.setInputArgs(inputArgs);
            }
            boolean localLast = !iter.hasNext();
            Command command = cf.newCommand(first, last && localLast);
            previous = command.getXMLFilter(new StaticArgFactory(commandEntry.getValue()), inputCommand, maxType);
            inputCommand = (InputCommandFactory.InputCommand) command.inputHandler();
            if (previous == null) {
                command.printHelpOn(System.err);
                return null;
            }
            if (localLast && !command.handlesOutput()) {
                localLast = false;
                iter = Collections.singletonMap((CommandFactory) ocf, new String[0]).entrySet().iterator();
            }
            inputBase = command.getInputBase();
            in = command.getInput();
            maxType = command.getCommandType();
            while (!localLast) {
                commandEntry = iter.next();
                cf = commandEntry.getKey();
                localLast = !iter.hasNext();
                Command lastCommand = command;
                command = cf.newCommand(false, last && localLast);
                XMLFilter child = command.getXMLFilter(new StaticArgFactory(commandEntry.getValue()), inputCommand, maxType);
                if (command.handlesOutput() && !(last && localLast)) {
                    localLast = true;
                    command = lastCommand;
                } else {
                    if (child == null) {
                        command.printHelpOn(System.err);
                        return null;
                    }
                    if (last && localLast && !command.handlesOutput()) {
                        localLast = false;
                        iter = Collections.singletonMap((CommandFactory) ocf, new String[0]).entrySet().iterator();
                    }
                    maxType = updateType(maxType, command.getCommandType());
                    getRootParent(child).setParent(previous);
                    previous = child;
                }
            }
            return new XMLFilterSource(previous, in, inputBase, maxType, command.handlesOutput(), command.inputHandler());
        } else {
            System.err.println("For help with a specific command: " + LS 
                    + "\t--command --help"+LS 
                    +"available commands: "+LS
                    +"\t"+CommandFactory.getAvailableCommandFactories().keySet());
            return null;
        }
    }
    
    private static void initLog4j() {
        PatternLayout layout = new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN);
        ConsoleAppender appender = new ConsoleAppender(layout, ConsoleAppender.SYSTEM_ERR);
        BasicConfigurator.configure(appender);
    }
    
    public XMLReader newXMLReader(List<String> args, ExecutorService executor, DataSourceFactory dsf) throws IOException {
        Map<String, CommandFactory> cfs = CommandFactory.getAvailableCommandFactories();
        Iterable<Map.Entry<CommandFactory, String[]>> commands = buildCommandList(args.toArray(new String[args.size()]), cfs);
        Iterator<Map.Entry<CommandFactory, String[]>> iter = commands.iterator();
        SAXSource source = chainCommands(true, null, iter, true);
        if (source == null) {
            return null;
        } else {
            XMLReader xmlReader = source.getXMLReader();
            try {
                xmlReader.setProperty(SAXProperties.EXECUTOR_SERVICE_PROPERTY_NAME, executor);
            } catch (SAXNotRecognizedException ex) {
                logger.trace("ignoring " + ex);
            } catch (SAXNotSupportedException ex) {
                logger.trace("ignoring " + ex);
            }
            try {
                xmlReader.setProperty(SAXProperties.DATA_SOURCE_FACTORY_PROPERTY_NAME, dsf);
            } catch (SAXNotRecognizedException ex) {
                logger.trace("ignoring " + ex);
            } catch (SAXNotSupportedException ex) {
                logger.trace("ignoring " + ex);
            }
            return xmlReader;
        }
    }

    public static void main(String[] args) throws IOException, TransformerConfigurationException {
        initLog4j();
        Map<String, CommandFactory> cfs = CommandFactory.getAvailableCommandFactories();
        Iterable<Map.Entry<CommandFactory, String[]>> commands = buildCommandList(args, cfs);
        Iterator<Map.Entry<CommandFactory, String[]>> iter = commands.iterator();
        SAXSource source = chainCommands(true, null, iter, true);
        if (source != null) {
            ExecutorService executor = null;
            try {
                XMLReader xmlReader = source.getXMLReader();
                executor = Executors.newCachedThreadPool();
                try {
                    xmlReader.setProperty(SAXProperties.EXECUTOR_SERVICE_PROPERTY_NAME, executor);
                } catch (SAXNotRecognizedException ex) {
                    executor.shutdown();
                    executor = null;
                    logger.trace("ignoring " + ex);
                } catch (SAXNotSupportedException ex) {
                    executor.shutdown();
                    executor = null;
                    logger.trace("ignoring " + ex);
                }
                xmlReader.parse(source.getInputSource());
            } catch (SAXException ex) {
                throw new RuntimeException(ex);
            } finally {
                if (executor != null) {
                    executor.shutdown();
                }
            }
        }
    }
    
    private static XMLFilter getRootParent(XMLFilter filter) {
        XMLReader parent;
        while ((parent = filter.getParent()) != null) {
            if (parent instanceof XMLFilter) {
                filter = (XMLFilter) parent;
            } else {
                return filter;
            }
        }
        return filter;
    }
    
    private static final String LS = System.lineSeparator();
    
    private static Iterable<Map.Entry<CommandFactory, String[]>> buildCommandList(String[] args, Map<String, CommandFactory> cfs) {
        LinkedList<Map.Entry<CommandFactory, String[]>> commandList = new LinkedList<Map.Entry<CommandFactory, String[]>>();
        CommandFactory current = null;
        int argStart = 0;
        for (int i = 0; i < args.length; i++) {
            if (args[i].startsWith("--")) {
                CommandFactory cf = cfs.get(args[i].substring(2));
                if (cf != null) {
                    if (current != null) {
                        commandList.add(new AbstractMap.SimpleImmutableEntry<CommandFactory, String[]>(current, Arrays.copyOfRange(args, argStart, i)));
                    } else if (i > 0) {
                        // pass extra leading arguments to induced input command factory
                        current = new InputCommandFactory();
                        commandList.add(new AbstractMap.SimpleImmutableEntry<CommandFactory, String[]>(current, Arrays.copyOfRange(args, argStart, i)));
                    }
                    argStart = i + 1;
                    current = cf;
                }
            }
        }
        if (current != null) {
            commandList.add(new AbstractMap.SimpleImmutableEntry<CommandFactory, String[]>(current, Arrays.copyOfRange(args, argStart, args.length)));
        }
        return commandList;
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