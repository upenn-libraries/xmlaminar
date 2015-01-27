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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.sax.SAXSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
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
            Class.forName(FileCommandFactory.class.getCanonicalName());
        } catch (ClassNotFoundException ex) {
            throw new RuntimeException(ex);
        }
        
    }
    
    public static CommandType updateType(CommandType current, CommandType next) {
        return (next.compareTo(current) > 0 ? next : current);
    }
    
    public static class XMLFilterSource extends SAXSource {

        private XMLFilter filter;
        private File inputBase;
        private CommandType commandType;

        public XMLFilterSource(XMLReader reader, InputSource inputSource, File inputBase, CommandType commandType) {
            this(reader, inputSource);
            this.inputBase = inputBase;
            this.commandType = commandType;
        }
        
        public XMLFilterSource(XMLReader reader, InputSource inputSource) {
            super(reader, inputSource);
            filter = readerToFilter(reader);
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
    
    public static XMLFilterSource chainCommands(boolean first, Iterator<Map.Entry<CommandFactory, String[]>> iter, boolean last) throws FileNotFoundException {
        XMLFilter previous;
        InputSource in;
        File inputBase;
        CommandType maxType = null;
        if (iter.hasNext()) {
            Map.Entry<CommandFactory, String[]> commandEntry = iter.next();
            CommandFactory cf = commandEntry.getKey();
            InputCommandFactory.InputCommand inputCommand = (InputCommandFactory.InputCommand) icf.newCommand(true, false);
            String[] inputArgs;
            if (cf instanceof InputCommandFactory) {
                inputArgs = commandEntry.getValue();
                if (!iter.hasNext()) {
                    inputCommand.printHelpOn(System.err);
                    return null;
                }
                commandEntry = iter.next();
            } else {
                inputArgs = new String[0];
            }
            boolean localLast = !iter.hasNext();
            Command command = commandEntry.getKey().newCommand(first, last && localLast);
            inputCommand.init(inputArgs, command.getCommandType());
            previous = command.getXMLFilter(commandEntry.getValue(), inputCommand, maxType);
            if (previous == null) {
                command.printHelpOn(System.err);
                return null;
            }
            inputBase = command.getInputBase();
            in = command.getInput();
            maxType = command.getCommandType();
            while (!localLast) {
                commandEntry = iter.next();
                localLast = !iter.hasNext();
                command = commandEntry.getKey().newCommand(false, last && localLast);
                XMLFilter child = command.getXMLFilter(commandEntry.getValue(), inputCommand, maxType);
                if (child == null) {
                    command.printHelpOn(System.err);
                    return null;
                }
                maxType = updateType(maxType, command.getCommandType());
                getRootParent(child).setParent(previous);
                previous = child;
            }
            return new XMLFilterSource(previous, in, inputBase, maxType);
        } else {
            System.err.println("For help with a specific command: " + LS 
                    + "\t--command --help"+LS 
                    +"available commands: "+LS
                    +"\t"+CommandFactory.getAvailableCommandFactories().keySet());
            return null;
        }
    }
    
    public static void main(String[] args) throws IOException, TransformerConfigurationException {
        Map<String, CommandFactory> cfs = CommandFactory.getAvailableCommandFactories();
        Iterable<Map.Entry<CommandFactory, String[]>> commands = buildCommandList(args, cfs);
        Iterator<Map.Entry<CommandFactory, String[]>> iter = commands.iterator();
        SAXSource source = chainCommands(true, iter, true);
        if (source != null) {
            try {
                source.getXMLReader().parse(source.getInputSource());
            } catch (SAXException ex) {
                throw new RuntimeException(ex);
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
        int argStart = -1;
        for (int i = 0; i < args.length; i++) {
            if (args[i].startsWith("--")) {
                CommandFactory cf = cfs.get(args[i].substring(2));
                if (cf != null) {
                    if (current != null) {
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
