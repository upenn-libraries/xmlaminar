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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;
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
public class ConfigCommandFactory extends CommandFactory {

    public static final String KEY = "config";
    private static final SAXParserFactory spf;

    static {
        registerCommandFactory(new ConfigCommandFactory());
        spf = SAXParserFactory.newInstance();
        spf.setNamespaceAware(true);

    }

    public ConfigCommandFactory() {
        this(true, false, null, null);
    }

    public ConfigCommandFactory(boolean direct, boolean first, InitCommand inputBase, CommandType maxType) {
        this.direct = direct;
        this.first = first;
        this.inputBase = inputBase;
        this.maxType = maxType;
    }

    private final boolean direct;
    private final boolean first;
    private final InitCommand inputBase;
    private final CommandType maxType;

    @Override
    public Command newCommand(boolean first, boolean last) {
        return newCommand(first, last, null);
    }

    @Override
    public Command newCommand(boolean first, boolean last, Properties overrides) {
        if (direct) {
            return new ConfigCommand(first, last, overrides);
        } else {
            Command backing;
            backing = wrappedCommandFactory.newCommand(first, last, overrides);
            Command.ArgFactory arf = new OverrideArgFactory(props, inputBase.recognizedOptions(), overrides);
            return new WrappedCommand(arf, inputBase, maxType, backing);
        }
    }

    private static class OverrideArgFactory implements Command.ArgFactory {

        private final Properties props;
        private final Set<String> recognizedInputOptions;
        private final Properties overrides;

        public OverrideArgFactory(Properties props, Set<String> recognizedInputOptions, Properties overrides) {
            this.props = props;
            this.recognizedInputOptions = recognizedInputOptions;
            this.overrides = overrides;
        }
        
        @Override
        public String[] getArgs(Set<String> recognizedOptions) {
            Set<String> rec = new HashSet<String>(recognizedInputOptions.size() + recognizedOptions.size());
            rec.addAll(recognizedOptions);
            rec.addAll(recognizedInputOptions);
            return constructCommandLineArgs(props, rec, overrides);
        }
        
    }
    
    @Override
    public String getKey() {
        return KEY;
    }

    static void verifyNamespaceURI(String uri) {
        if (!Driver.CONFIG_NAMESPACE_URI.equals(uri)) {
            throw new IllegalStateException("bad namespace uri; expected "
                    + Driver.CONFIG_NAMESPACE_URI + ", found " + uri);
        }
    }

    @Override
    public CommandFactory getConfiguringXMLFilter(boolean first, InitCommand inputBase, CommandType maxType) {
        return new ConfigCommandFactory(false, first, inputBase, maxType);
    }

    private static String[] constructCommandLineArgs(Properties props, Set<String> recognizedOptions, Properties overrides) {
        ArrayList<String> ret = new ArrayList<String>(props.size() * 2);
        int i = 0;
        for (String s : props.stringPropertyNames()) {
            if (overrides == null || !overrides.containsKey(s)) {
                String val = props.getProperty(s);
                if ((val == null || "".equals(val)) && !recognizedOptions.contains(s)) {
                    ret.add(s);
                } else {
                    ret.add("--".concat(s));
                    ret.add(val);
                }
            }
        }
        if (overrides != null) {
            for (String s : overrides.stringPropertyNames()) {
                if (recognizedOptions.contains(s)) {
                    String val = overrides.getProperty(s);
                    if ((val == null || "".equals(val))) {
                        ret.add(s);
                    } else {
                        ret.add("--".concat(s));
                        ret.add(val);
                    }
                }
            }
        }
        return ret.toArray(new String[ret.size()]);
    }
    
    String[] constructCommandLineArgs(InitCommand inputBase) {
        return constructCommandLineArgs(props, inputBase.recognizedOptions(), null);
    }

    private ConfigCommandFactory configure(InputSource configSource) {
        try {
            setParent(spf.newSAXParser().getXMLReader());
            parse(configSource);
        } catch (ParserConfigurationException ex) {
            throw new RuntimeException(ex);
        } catch (SAXException ex) {
            throw new RuntimeException(ex);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        return this;
    }

    private int depth = -1;
    private final Properties props = new Properties();
    private int delegateDepth = Integer.MAX_VALUE;
    private CommandFactory wrappedCommandFactory;

    private void reset() {
        props.clear();
        depth = -1;
        delegateDepth = Integer.MAX_VALUE;
        propsBuilder.setLength(0);
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
    public void endElement(String uri, String localName, String qName) throws SAXException {
        if (--depth == 0) {
            registerTextProps();
        } else if (depth == 1) {
            props.setProperty(workingPropName, propsBuilder.toString());
            workingPropName = null;
            propsBuilder.setLength(0);
        }
        super.endElement(uri, localName, qName);
    }

    private final Map<String, CommandFactory> cfs = CommandFactory.getAvailableCommandFactories();

    private static final Set<String> VALID_LOCAL_NAMES = Collections.unmodifiableSet(new HashSet<String>(Arrays.asList(new String[] {"source", "filter"})));
    
    @Override
    public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
        verifyNamespaceURI(uri);
        if (depth == 0) {
            if (first && !"source".equals(localName)) {
                throw new IllegalStateException("bad element localName for first=" + first + "; expected source"
                        + ", found " + localName+", type="+atts.getValue("type"));
            } else if (!VALID_LOCAL_NAMES.contains(localName)) {
                throw new IllegalStateException("bad element localName for first=" + first + "; expected "
                        + VALID_LOCAL_NAMES + ", found " + localName+", type="+atts.getValue("type"));
            }
            String type;
            CommandFactory cf = cfs.get(type = atts.getValue("type"));
            if (cf == null) {
                throw new IllegalArgumentException("type must be one of " + cfs + "; found " + type);
            } else if (cf instanceof ConfigCommandFactory) {
                wrappedCommandFactory = null;
            } else {
                wrappedCommandFactory = cf.getConfiguringXMLFilter(first, inputBase, maxType);
            }
            if (wrappedCommandFactory != null) {
                delegateDepth = depth;
                XMLReader parent = getParent();
                parent.setContentHandler(passThrough);
                passThrough.setParent(parent);
                passThrough.setContentHandler(wrappedCommandFactory);
                wrappedCommandFactory.setParent(passThrough);
                wrappedCommandFactory.startDocument();
                wrappedCommandFactory.startElement(uri, localName, qName, atts);
            } else {
                wrappedCommandFactory = cf;
                super.startElement(uri, localName, qName, atts);
            }
        } else if (depth == 1) {
            registerTextProps();
            if ("property".equals(localName)) {
                String name = atts.getValue("name");
                if (name == null) {
                    throw new IllegalArgumentException("must specify name attribute for property element");
                }
                workingPropName = name;
                propsBuilder.setLength(0);
            } else if ("properties".equals(localName)) {
                Properties newProps = new Properties();
                try {
                    newProps.load(new FileInputStream(atts.getValue("path")));
                } catch (IOException ex) {
                    throw new RuntimeException("not a valid path: ", ex);
                }
                props.putAll(newProps);
            }
            super.startElement(uri, localName, qName, atts);
        } else {
            throw new IllegalStateException("cannot handle depth greater than 1");
        }
        depth++;
    }

    private String workingPropName = null;
    private final StringBuilder propsBuilder = new StringBuilder();

    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        if (depth == 1) {
            propsBuilder.append(ch, start, length);
        } else if (depth == 2) {
            if (workingPropName == null) {
                throw new AssertionError("this should never happen");
            }
            propsBuilder.append(ch, start, length);
        }
        super.characters(ch, start, length);
    }

    private void registerTextProps() {
        Properties newProps = new Properties();
        try {
            newProps.load(new StringReader(propsBuilder.toString()));
        } catch (IOException ex) {
            throw new RuntimeException("this should never happen", ex);
        }
        propsBuilder.setLength(0);
        props.putAll(newProps);
    }

    private final PassThroughXMLFilter passThrough = new PassThroughXMLFilter();

    private class PassThroughXMLFilter extends XMLFilterImpl {

        @Override
        public void endElement(String uri, String localName, String qName) throws SAXException {
            super.endElement(uri, localName, qName);
            if (--depth <= delegateDepth) {
                super.endDocument();
                delegateDepth = Integer.MAX_VALUE;
                getParent().setContentHandler(ConfigCommandFactory.this);
            }
        }

        @Override
        public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
            super.startElement(uri, localName, qName, atts);
            depth++;
        }

    }

    private static class WrappedCommand<T extends InitCommand> implements Command<T> {

        private final ArgFactory arf;
        private final T inputBase;
        private final CommandType maxType;
        private final Command<T> backing;

        public WrappedCommand(ArgFactory arf, T inputBase, CommandType maxType, Command backing) {
            this.arf = arf;
            this.inputBase = inputBase;
            this.maxType = maxType;
            this.backing = backing;
        }
        
        @Override
        public XMLFilter getXMLFilter(ArgFactory arf, T inputBase, CommandType maxType) {
            return backing.getXMLFilter(this.arf, this.inputBase, this.maxType);
        }

        @Override
        public InputSource getInput() throws FileNotFoundException {
            return backing.getInput();
        }

        @Override
        public File getInputBase() {
            return backing.getInputBase();
        }

        @Override
        public void printHelpOn(OutputStream out) {
            backing.printHelpOn(out);
        }

        @Override
        public CommandType getCommandType() {
            return backing.getCommandType();
        }

        @Override
        public boolean handlesOutput() {
            return backing.handlesOutput();
        }

        @Override
        public InitCommand inputHandler() {
            return backing.inputHandler();
        }

    }
    
    private static class ConfigCommand<T extends Command & InitCommand> implements Command<T> {

        private static final Set<String> helpArgs;
        private InputSource configSource;
        private final boolean first;
        private final boolean last;
        private Command inputBase;
        private CommandType maxType;
        private String[] args;
        private Command<T> backing;
        private final Properties overrides;

        static {
            Set<String> tmp = new HashSet<String>(2);
            tmp.add("-h");
            tmp.add("--help");
            helpArgs = Collections.unmodifiableSet(tmp);
        }

        public ConfigCommand(boolean first, boolean last, Properties overrides) {
            this.first = first;
            this.last = last;
            this.overrides = overrides;
        }

        private boolean parseArgs(String[] args) {
            if (args.length < 1 || helpArgs.contains(args[0])) {
                return false;
            }
            File f = new File(args[0]);
            if ("-".equals(f.getPath())) {
                configSource = new InputSource(System.in);
            } else if (f.isFile()) {
                try {
                    configSource = new InputSource(new BufferedInputStream(new FileInputStream(f)));
                    configSource.setSystemId(f.getAbsolutePath());
                } catch (FileNotFoundException ex) {
                    throw new IllegalArgumentException(ex);
                }
            } else {
                throw new IllegalArgumentException(f + " is not a regular file");
            }
            return true;
        }

        @Override
        public XMLFilter getXMLFilter(ArgFactory arf, T inputBase, CommandType maxType) {
            if (backing != null) {
                return backing.getXMLFilter(arf, inputBase, maxType);
            }
            this.inputBase = inputBase;
            this.maxType = maxType;
            this.args = arf.getArgs(Collections.EMPTY_SET);
            if (!parseArgs(args)) {
                return null;
            }
            backing = new ConfigCommandFactory(false, first, inputBase, maxType).configure(configSource).newCommand(first, last, overrides);
            return backing.getXMLFilter(null, inputBase, maxType);
        }

        @Override
        public CommandType getCommandType() {
            return backing.getCommandType();
        }

        @Override
        public File getInputBase() {
            return backing.getInputBase();
        }

        @Override
        public InputSource getInput() throws FileNotFoundException {
            return backing.getInput();
        }

        @Override
        public void printHelpOn(OutputStream out) {
            PrintStream ps = new PrintStream(out);
            ps.println("command \"" + KEY + "\" accepts single config file argument");
        }

        @Override
        public boolean handlesOutput() {
            return backing.handlesOutput();
        }

        @Override
        public InitCommand inputHandler() {
            return backing.inputHandler();
        }

    }

}
