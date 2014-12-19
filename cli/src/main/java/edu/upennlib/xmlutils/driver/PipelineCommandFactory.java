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

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package edu.upennlib.xmlutils.driver;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.OutputStream;
import java.util.AbstractMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLFilter;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLFilterImpl;

/**
 *
 * @author magibney
 */
public class PipelineCommandFactory extends CommandFactory {

    private static final String KEY = "pipeline";
    
    static {
        registerCommandFactory(new PipelineCommandFactory());
    }

    @Override
    public Command newCommand(boolean first, boolean last) {
        return new PipelineCommand(first, last);
    }

    @Override
    public String getKey() {
        return KEY;
    }

    private static class PipelineCommand<T extends XMLFilter & ContentHandler> extends XMLFilterImpl implements Command {

        private final boolean first;
        private final boolean last;
        private final LinkedList<Entry<Command, String[]>> commands = new LinkedList<Entry<Command, String[]>>();
        private int depth = -1;
        private File inputBase;
        private CommandType maxType;
        
        public PipelineCommand(boolean first, boolean last) {
            this.first = first;
            this.last = last;
        }

        @Override
        public XMLFilter getXMLFilter(String[] args, File inputBase, CommandType maxType) {
            try {
                return Driver.chainCommands(commands.iterator()).getXMLReader();
            } catch (FileNotFoundException ex) {
                throw new RuntimeException(ex);
            }
        }

        @Override
        public XMLFilterImpl getConfiguringXMLFilter(File inputBase, CommandType maxType) {
            this.inputBase = inputBase;
            this.maxType = maxType;
            return this;
        }

        private void reset() {
            depth = -1;
            commands.clear();
        }
        
        @Override
        public void endElement(String uri, String localName, String qName) throws SAXException {
            depth--;
            super.endElement(uri, localName, qName);
        }

        private final Map<String, CommandFactory> cfs = CommandFactory.getAvailableCommandFactories();
        
        private int delegateDepth = Integer.MAX_VALUE;
        
        @Override
        public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
            ConfigCommandFactory.verifyNamespaceURI(uri);
            if (depth == 0) {
                if (!localName.equals(first ? "source" : "filter")) {
                    throw new IllegalStateException("bad element localName for first=" + first + "; expected "
                            + (first ? "source" : "filter") + ", found " + localName);
                }
                super.startElement(uri, localName, qName, atts);
            } else if (depth == 1) {
                if (!localName.equals(first && commands.isEmpty() ? "source" : "filter")) {
                    throw new IllegalStateException("bad element localName for first=" + first + "; expected "
                            + (first ? "source" : "filter") + ", found " + localName);
                }
                String type;
                CommandFactory cf = cfs.get(type = atts.getValue("type"));
                if (cf == null) {
                    throw new IllegalArgumentException("type must be one of " + cfs + "; found " + type);
                }
                Command currentCommand = cf.newCommand(first && commands.isEmpty(), last);
                T cch = currentCommand.getConfiguringXMLFilter(inputBase, maxType);
                if (cch != null) {
                    delegateDepth = depth;
                    XMLReader parent = getParent();
                    parent.setContentHandler(passThrough);
                    passThrough.setParent(parent);
                    passThrough.setContentHandler(cch);
                    cch.setParent(passThrough);
                    cch.startDocument();
                    cch.startElement(uri, localName, qName, atts);
                } else {
                    currentCommand = new ConfigCommandFactory().newCommand(first && commands.isEmpty(), last);
                    cch = currentCommand.getConfiguringXMLFilter(inputBase, maxType);
                    delegateDepth = depth;
                    XMLReader parent = getParent();
                    parent.setContentHandler(passThrough);
                    passThrough.setParent(parent);
                    passThrough.setContentHandler(cch);
                    cch.setParent(passThrough);
                    cch.startDocument();
                    cch.startElement(uri, localName, qName, atts);
                }
                commands.add(new AbstractMap.SimpleImmutableEntry<Command, String[]>(currentCommand, null));
            } else {
                throw new AssertionError("this should never happen");
            }
            depth++;
        }
        
        private static final String[] EMPTY = new String[0];

        private final PassThroughXMLFilter passThrough = new PassThroughXMLFilter();

        private class PassThroughXMLFilter extends XMLFilterImpl {

            @Override
            public void endElement(String uri, String localName, String qName) throws SAXException {
                super.endElement(uri, localName, qName);
                if (--depth <= delegateDepth) {
                    super.endDocument();
                    delegateDepth = Integer.MAX_VALUE;
                    getParent().setContentHandler(PipelineCommand.this);
                }
            }

            @Override
            public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
                super.startElement(uri, localName, qName, atts);
                depth++;
            }

        }

        @Override
        public void endDocument() throws SAXException {
            depth--;
            super.endDocument();
        }

        @Override
        public void startDocument() throws SAXException {
            reset();
            super.startDocument();
            depth++;
        }
        
        @Override
        public InputSource getInput() throws FileNotFoundException {
            return commands.getFirst().getKey().getInput();
        }

        @Override
        public File getInputBase() {
            return commands.getFirst().getKey().getInputBase();
        }

        @Override
        public void printHelpOn(OutputStream out) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public CommandType getCommandType() {
            return commands.getLast().getKey().getCommandType();
        }
    }
    
}
