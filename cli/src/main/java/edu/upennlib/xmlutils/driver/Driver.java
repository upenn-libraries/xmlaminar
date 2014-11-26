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
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import javax.xml.transform.TransformerConfigurationException;
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

    private static final Logger logger = LoggerFactory.getLogger(Driver.class);
    
    static {
        try {
            // Force initialization of known Command types.
            Class.forName(SplitCommandFactory.class.getCanonicalName());
            Class.forName(ProcessCommandFactory.class.getCanonicalName());
            Class.forName(JoinCommandFactory.class.getCanonicalName());
        } catch (ClassNotFoundException ex) {
            throw new RuntimeException(ex);
        }
        
    }
    
    private static CommandType updateType(CommandType current, CommandType next) {
        return (next.compareTo(current) > 0 ? next : current);
    }
    
    public static void main(String[] args) throws IOException, TransformerConfigurationException {
        Map<String, CommandFactory> cfs = CommandFactory.getAvailableCommandFactories();
        LinkedList<Command> commands = buildCommandList(args, cfs);
        Iterator<Command> iter = commands.iterator();
        XMLFilter previous;
        InputSource in;
        File inputBase;
        CommandType maxType = null;
        if (iter.hasNext()) {
            Command command = iter.next();
            previous = command.getXMLFilter(null, maxType);
            if (previous == null) {
                command.printHelpOn(System.err);
                return;
            }
            inputBase = command.getInputBase();
            in = command.getInput();
            maxType = command.getCommandType();
            while (iter.hasNext()) {
                command = iter.next();
                XMLFilter child = command.getXMLFilter(inputBase, maxType);
                if (child == null) {
                    command.printHelpOn(System.err);
                    return;
                }
                maxType = updateType(maxType, command.getCommandType());
                getRootParent(child).setParent(previous);
                previous = child;
            }
        } else {
            System.err.println("For help with a specific command: " + LS 
                    + "\t--command --help"+LS 
                    +"available commands: "+LS
                    +"\t"+cfs.keySet());
            return;
        }
        try {
            previous.parse(in);
        } catch (SAXException ex) {
            throw new RuntimeException(ex);
        }
        System.out.println();
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
    
    private static LinkedList<Command> buildCommandList(String[] args, Map<String, CommandFactory> cfs) {
        LinkedList<Command> commandList = new LinkedList<Command>();
        CommandFactory current = null;
        int argStart = -1;
        boolean first = true;
        for (int i = 0; i < args.length; i++) {
            if (args[i].startsWith("--")) {
                CommandFactory cf = cfs.get(args[i].substring(2));
                if (cf != null) {
                    if (current != null) {
                        commandList.add(current.newCommand(Arrays.copyOfRange(args, argStart, i), first, false));
                        first = false;
                    }
                    argStart = i + 1;
                    current = cf;
                }
            }
        }
        if (current != null) {
            commandList.add(current.newCommand(Arrays.copyOfRange(args, argStart, args.length), first, true));
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
