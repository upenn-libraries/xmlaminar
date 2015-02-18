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

import edu.upennlib.ingestor.sax.integrator.IntegratorOutputNode;
import edu.upennlib.ingestor.sax.integrator.StatefulXMLFilter;
import edu.upennlib.paralleltransformer.InputSourceXMLReader;
import edu.upennlib.paralleltransformer.InputSplitter;
import edu.upennlib.paralleltransformer.JoiningXMLFilter;
import edu.upennlib.xmlutils.dbxml.SQLXMLReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.AbstractMap;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLFilter;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLFilterImpl;

/**
 *
 * @author magibney
 */
public class IntegrateCommandFactory extends CommandFactory {

    private static final String KEY = "integrate";

    static {
        registerCommandFactory(new IntegrateCommandFactory());
    }

    private final Map<String, CommandFactory> cfs = CommandFactory.getAvailableCommandFactories();

    private InitCommand inputBase;
    private CommandType maxType;
    
    private int depth = -1;
    private int delegateDepth = Integer.MAX_VALUE;
    private IntegratorOutputNode root;

    private void reset() {
        root = new IntegratorOutputNode();
    }
    
    @Override
    public CommandFactory getConfiguringXMLFilter(boolean first, InitCommand inputBase, CommandType maxType) {
        if (!first) {
            throw new IllegalArgumentException();
        }
        this.inputBase = inputBase;
        this.maxType = maxType;
        return this;
    }

    @Override
    public void startDocument() throws SAXException {
        reset();
        super.startDocument();
        depth++;
    }

    @Override
    public void endDocument() throws SAXException {
        depth--;
        super.endDocument();
    }

    @Override
    public Command newCommand(boolean first, boolean last) {
        if (!first) {
            throw new IllegalArgumentException();
        }
        return new IntegrateCommand(first, last);
    }

    private final ArrayDeque<Entry<String, Boolean>> outputElementStack = new ArrayDeque<Entry<String, Boolean>>();
    private CommandFactory delegateCommandFactory;
    private final Properties overrides = new Properties();
    
    private String parsePropName;
    private final StringBuilder propBuilder = new StringBuilder();
    
    @Override
    public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
        if (!Driver.CONFIG_NAMESPACE_URI.equals(uri)) {
            String required = atts.getValue(Driver.CONFIG_NAMESPACE_URI, "required");
            outputElementStack.add(new AbstractMap.SimpleImmutableEntry<String, Boolean>(localName, required == null ? false : Boolean.parseBoolean(required)));
        } else if ("property".equals(localName)) {
            parsePropName = atts.getValue("name");
            if (parsePropName != null) {
                parsePropName = parsePropName.trim();
                if (parsePropName.length() < 1) {
                    parsePropName = null;
                } else {
                    propBuilder.setLength(0);
                }
            }
        } else if (!"source".equals(localName)) {
            throw new IllegalStateException("bad element localName for integrator source; expected \"source\", found " + localName);
        } else if (depth == 0) {
            if (!"integrate".equals(atts.getValue("type"))) {
                throw new IllegalStateException("bad type for integrator source; expected \"integrate\", found " + atts.getValue("type"));
            }
        } else {
            String type;
            CommandFactory cf = cfs.get(type = atts.getValue("type"));
            if (cf == null) {
                throw new IllegalArgumentException("type must be one of " + cfs + "; found " + type);
            }
            delegateCommandFactory = cf.getConfiguringXMLFilter(true, inputBase, maxType);
            if (delegateCommandFactory == null) {
                delegateCommandFactory = new ConfigCommandFactory(false, true, inputBase, maxType);
            }
            delegateDepth = depth;
            XMLReader parent = getParent();
            parent.setContentHandler(passThrough);
            passThrough.setParent(parent);
            passThrough.setContentHandler(delegateCommandFactory);
            delegateCommandFactory.setParent(passThrough);
            delegateCommandFactory.startDocument();
            delegateCommandFactory.startElement(uri, localName, qName, atts);
        }
        super.startElement(uri, localName, qName, atts);
        depth++;
    }

    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        super.characters(ch, start, length);
        if (parsePropName != null) {
            propBuilder.append(ch, start, length);
        }
    }
    
    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        depth--;
        if (!Driver.CONFIG_NAMESPACE_URI.equals(uri)) {
            outputElementStack.removeLast();
        } else if (parsePropName != null) {
            if ("property".equals(localName)) {
                overrides.setProperty(parsePropName, propBuilder.toString());
            }
            parsePropName = null;
        }
        super.endElement(uri, localName, qName);
    }

    private final PassThroughXMLFilter passThrough = new PassThroughXMLFilter();

    private static final InputSource dummy = new InputSource();
    private static final boolean EXPECT_INPUT = false;
    
    private class IntegrateCommand implements Command {

        private final boolean first;
        private final boolean last;
        private XMLFilter ret;
        private InitCommand inputBase;
        
        public IntegrateCommand(boolean first, boolean last) {
            this.first = first;
            this.last = last;
        }

        @Override
        public XMLFilter getXMLFilter(ArgFactory arf, InitCommand inputBase, CommandType maxType) {
            if (ret == null) {
                if (!CommandFactory.conditionalInit(first, inputBase, EXPECT_INPUT)) {
                    return null;
                }
                this.inputBase = inputBase;
                int lookaheadFactor = Integer.parseInt(overrides.getProperty("lookahead", "0"));
                JoiningXMLFilter joiner = new JoiningXMLFilter(true);
                XMLReader inputHandler;
                switch (sxfs.size()) {
                    case 0:
                        inputHandler = new SXFResetter(root);
                        break;
                    case 1:
                        inputHandler = new InputSetter(root, sxfs.get(0));
                        joiner.setIteratorWrapper(new InputSplitter(actualBatchSize, lookaheadFactor, false));
                        break;
                    default:
                        inputHandler = new InputMultiplier(root, sxfs.toArray(new StatefulXMLFilter[sxfs.size()]));
                        joiner.setIteratorWrapper(new InputSplitter(actualBatchSize, lookaheadFactor, true));
                }
                joiner.setParent(inputHandler);
                ret = joiner;
                //ret = new XMLFilterImpl(root);
            }
            return ret;
        }

        @Override
        public InputSource getInput() throws FileNotFoundException {
            if (first) {
                return inputBase.getInput();
            } else {
                return null;
            }
        }

        @Override
        public File getInputBase() {
            return null;
        }

        @Override
        public void printHelpOn(OutputStream out) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public CommandType getCommandType() {
            return CommandType.PASS_THROUGH;
        }

        @Override
        public boolean handlesOutput() {
            return false;
        }

        @Override
        public InitCommand inputHandler() {
            return inputBase;
        }
    }

    private static IntegratorOutputNode firstIntegratorParent(XMLReader reader) {
        do {
            if (reader == null) {
                return null;
            } else if (reader instanceof IntegratorOutputNode) {
                return (IntegratorOutputNode) reader;
            }
        } while (reader instanceof XMLFilter && (reader = ((XMLFilter)reader).getParent()) != null);
        return null;
    }
    
    private static class InputMultiplier extends InputSetter {

        private final StatefulXMLFilter[] children;
        
        public InputMultiplier(XMLReader parent, StatefulXMLFilter[] children) {
            super(parent, (children == null ? null : children[0]));
            if (children == null || children.length < 2) {
                this.children = null;
            } else {
                this.children = Arrays.copyOfRange(children, 1, children.length);
            }
        }

        @Override
        public void parse(InputSource input) throws SAXException, IOException {
            if (children != null) {
                InputSplitter.ForkableInputSource fis = (InputSplitter.ForkableInputSource) input;
                for (StatefulXMLFilter sxf : children) {
                    sxf.setInputSource(fis.fork());
                }
            }
            super.parse(input);
        }

    }

    private static class InputSetter extends SXFResetter {

        private final StatefulXMLFilter sxf;
        
        public InputSetter(XMLReader parent, StatefulXMLFilter sxf) {
            super(parent);
            this.sxf = sxf;
        }

        @Override
        public void setParent(XMLReader parent) {
            super.setParent(parent);
        }
        
        @Override
        public void parse(InputSource input) throws SAXException, IOException {
            sxf.setInputSource(input);
            super.parse(input);
        }

    }

    private static class SXFResetter extends XMLFilterImpl {

        private IntegratorOutputNode ion;
        
        public SXFResetter(XMLReader parent) {
            super(parent);
            this.ion = firstIntegratorParent(parent);
        }

        @Override
        public void setParent(XMLReader parent) {
            this.ion = firstIntegratorParent(parent);
            super.setParent(parent);
        }
        
        @Override
        public void parse(InputSource input) throws SAXException, IOException {
            ion.reset();
            super.parse(input);
        }

    }

    private class PassThroughXMLFilter extends XMLFilterImpl {

        @Override
        public void endElement(String uri, String localName, String qName) throws SAXException {
            super.endElement(uri, localName, qName);
            if (--depth <= delegateDepth) {
                super.endDocument();
                delegateDepth = Integer.MAX_VALUE;
                getParent().setContentHandler(IntegrateCommandFactory.this);
                addNode();
            }
        }

        @Override
        public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
            super.startElement(uri, localName, qName, atts);
            depth++;
        }

    }
    
    private final List<StatefulXMLFilter> sxfs = new ArrayList<StatefulXMLFilter>();
    private int actualBatchSize = -1;
    
    private void addNode() {
        Command command = delegateCommandFactory.newCommand(true, false, overrides);
        XMLFilter xmlFilter = command.getXMLFilter(null, inputBase, maxType);
        XMLFilter inputConfigured;
        if (xmlFilter instanceof SQLXMLReader) {
            inputConfigured = xmlFilter;
        } else {
            try {
                inputConfigured = new InputSourceXMLReader(xmlFilter, command.getInput());
            } catch (FileNotFoundException ex) {
                throw new RuntimeException(ex);
            }
        }
        LinkedList<String> pathElements = new LinkedList<String>();
        for (Entry<String, Boolean> e : outputElementStack) {
            pathElements.add(e.getKey());
        }
        StatefulXMLFilter sxf = root.addDescendent(pathElements, inputConfigured, outputElementStack.peekLast().getValue());
        if (xmlFilter instanceof SQLXMLReader) {
            SQLXMLReader sxr = (SQLXMLReader) xmlFilter;
            if (sxr.isParameterized()) {
                sxfs.add(sxf);
                int localBatchSize = sxr.getBatchSize();
                if (actualBatchSize < 0) {
                    actualBatchSize = localBatchSize;
                } else if (actualBatchSize != localBatchSize) {
                    throw new IllegalStateException("incompatible batch sizes!: "+actualBatchSize +" != "+localBatchSize);
                }
            }
        }
        delegateCommandFactory = null;
    }

    @Override
    public String getKey() {
        return KEY;
    }

}
