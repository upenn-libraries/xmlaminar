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
import java.io.IOException;
import java.io.OutputStream;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;
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

    private final boolean xmlConfigured;
    private final boolean first;
    private InitCommand inputBase;
    private final CommandType maxType;
    
    public PipelineCommandFactory() {
        this(false, false, null, null);
    }
    
    private PipelineCommandFactory(boolean first, InitCommand inputBase, CommandType maxType) {
        this(true, first, inputBase, maxType);
    }
    
    private PipelineCommandFactory(boolean xmlConfigured, boolean first, InitCommand inputBase, CommandType maxType) {
        this.xmlConfigured = xmlConfigured;
        this.first = first;
        this.inputBase = inputBase;
        this.maxType = maxType;
    }
    
    @Override
    public PipelineCommandFactory getConfiguringXMLFilter(boolean first, InitCommand inputBase, CommandType maxType) {
        return new PipelineCommandFactory(first, inputBase, maxType);
    }

    @Override
    public Command newCommand(boolean first, boolean last) {
        if (!xmlConfigured) {
            throw new IllegalStateException();
        }
        return new PipelineCommand(this.first, last);
    }

    @Override
    public String getKey() {
        return KEY;
    }

    private final LinkedList<Entry<CommandFactory, String[]>> commandFactories = new LinkedList<Entry<CommandFactory, String[]>>();
    private int depth = -1;

    private void reset() {
        depth = -1;
        commandFactories.clear();
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
            if (!localName.equals(first && commandFactories.isEmpty() ? "source" : "filter")) {
                throw new IllegalStateException("bad element localName for first=" + first + "; expected "
                        + (first ? "source" : "filter") + ", found " + localName);
            }
            String type;
            CommandFactory cf = cfs.get(type = atts.getValue("type"));
            if (cf == null) {
                throw new IllegalArgumentException("type must be one of " + cfs + "; found " + type);
            }
            CommandFactory currentCommandFactory = cf.getConfiguringXMLFilter(first && commandFactories.isEmpty(), inputBase, maxType);
            if (currentCommandFactory == null) {
                currentCommandFactory = new ConfigCommandFactory(true, first && commandFactories.isEmpty(), inputBase, maxType)
                        .getConfiguringXMLFilter(first && commandFactories.isEmpty(), inputBase, maxType);
            }
            delegateDepth = depth;
            XMLReader parent = getParent();
            parent.setContentHandler(passThrough);
            passThrough.setParent(parent);
            passThrough.setContentHandler(currentCommandFactory);
            currentCommandFactory.setParent(passThrough);
            currentCommandFactory.startDocument();
            currentCommandFactory.startElement(uri, localName, qName, atts);
            if (cf instanceof InputCommandFactory) {
                inputBase = (InputCommandFactory.InputCommand) cf.newCommand(first, false);
                inputCommandFactory = (ConfigCommandFactory) currentCommandFactory;
            } else {
                commandFactories.add(new AbstractMap.SimpleImmutableEntry<CommandFactory, String[]>(currentCommandFactory, null));
            }
        } else {
            throw new AssertionError("this should never happen");
        }
        depth++;
    }
    
    private ConfigCommandFactory inputCommandFactory;

    private final PassThroughXMLFilter passThrough = new PassThroughXMLFilter();

    private class PassThroughXMLFilter extends XMLFilterImpl {

        @Override
        public void endElement(String uri, String localName, String qName) throws SAXException {
            super.endElement(uri, localName, qName);
            if (--depth <= delegateDepth) {
                super.endDocument();
                delegateDepth = Integer.MAX_VALUE;
                getParent().setContentHandler(PipelineCommandFactory.this);
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

    private class PipelineCommand implements Command<InputCommandFactory.InputCommand> {

        private final boolean first;
        private final boolean last;
        private Driver.XMLFilterSource<InputCommandFactory.InputCommand> xmlFilterSource;

        public PipelineCommand(boolean first, boolean last) {
            this.first = first;
            this.last = last;
        }

        @Override
        public XMLFilter getXMLFilter(String[] args, InputCommandFactory.InputCommand inputBase, CommandType maxType) {
            try {
                if (inputCommandFactory != null) {
                    args = inputCommandFactory.constructCommandLineArgs(PipelineCommandFactory.this.inputBase);
                    System.out.println("blah: " + Arrays.asList(args));
                    PipelineCommandFactory.this.inputBase.setInputArgs(args);
                }
                xmlFilterSource = Driver.chainCommands(first, PipelineCommandFactory.this.inputBase, commandFactories.iterator(), last);
            } catch (FileNotFoundException ex) {
                throw new RuntimeException(ex);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
            return xmlFilterSource.getXMLReader();
        }

        @Override
        public InputSource getInput() throws FileNotFoundException {
            return xmlFilterSource.getInputSource();
        }

        @Override
        public File getInputBase() {
            return xmlFilterSource.getInputBase();
        }

        @Override
        public void printHelpOn(OutputStream out) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public CommandType getCommandType() {
            return xmlFilterSource.getCommandType();
        }

        @Override
        public boolean handlesOutput() {
            return xmlFilterSource.handlesOutput();
        }

        @Override
        public InputCommandFactory.InputCommand inputHandler() {
            return xmlFilterSource.inputHandler();
        }
    }

}
