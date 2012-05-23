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

package edu.upennlib.ingestor.sax.integrator;

import edu.upennlib.configurationutils.IndexedPropertyConfigurable;
import edu.upennlib.xmlutils.DumpingContentHandler;
import edu.upennlib.xmlutils.UnboundedContentHandlerBuffer;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.logging.Level;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.sax.SAXResult;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.sax.SAXTransformerFactory;
import javax.xml.transform.stream.StreamResult;
import org.apache.log4j.Logger;
import org.xml.sax.ContentHandler;
import org.xml.sax.DTDHandler;
import org.xml.sax.EntityResolver;
import org.xml.sax.ErrorHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXNotSupportedException;
import org.xml.sax.XMLReader;
import org.xml.sax.ext.LexicalHandler;
import org.xml.sax.helpers.AttributesImpl;
import org.xml.sax.helpers.XMLFilterImpl;

/**
 *
 * @author michael
 */
public class IntegratorOutputNode implements IdQueryable, XMLReader {

    private final boolean debugging = true;
    private final StatefulXMLFilter inputFilter;
    private ContentHandler output;
    private ContentHandler rawOutput;
    private String[] childElementNames;
    private IdQueryable[] childNodes;
    private Boolean[] requireForWrite;
    private String name;
    private File dumpFile;
    private static final Logger logger = Logger.getLogger(IntegratorOutputNode.class);

    public File getDumpFile() {
        return dumpFile;
    }

    public void setDumpFile(File file) {
        dumpFile = file;
    }

    private List<String> descendentsSpring = null;
    public void setDescendentsSpring(List<String> descendentsSpring) {
        this.descendentsSpring = descendentsSpring;
    }
    public List<String> getDescendentsSpring() {
        return descendentsSpring;
    }

    private List<XMLReader> subIntegratorsSpring = null;
    public void setSubIntegratorsSpring(List<XMLReader> writeDuplicateIdsSpring) {
        this.subIntegratorsSpring = writeDuplicateIdsSpring;
    }
    public List<XMLReader> getSubIntegratorsSpring() {
        return subIntegratorsSpring;
    }

    public void initSpring() {
        HashMap<String, XMLReader> subIntegrators = new HashMap<String, XMLReader>();
        for (XMLReader integrator : subIntegratorsSpring) {
            if (!(integrator instanceof IndexedPropertyConfigurable)) {
                throw new RuntimeException(integrator+" not an instance of "+IndexedPropertyConfigurable.class.getCanonicalName());
            }
            IndexedPropertyConfigurable ipc = (IndexedPropertyConfigurable) integrator;
            if (ipc.getName() == null) {
                throw new RuntimeException("integrator "+integrator+" must have non-null name");
            }
            if (subIntegrators.put(ipc.getName(), integrator) != null) {
                throw new RuntimeException("integrator " + ipc.getName() + " (" + integrator + ") specified multiple times");
            }
        }
        for (String s : descendentsSpring) {
            String[] args = s.split("\\s*,\\s*");
            if (args.length != 3) {
                throw new IllegalArgumentException();
            }
            addDescendent(args[0], (XMLReader) subIntegrators.get(args[1]), Boolean.parseBoolean(args[2]));
        }
    }

    public void addDescendent(String path, XMLReader source, boolean requireForWrite) {
        String[] pe = path.substring(1).split("/");
        LinkedList<String> pathElements = new LinkedList<String>(Arrays.asList(pe));
        addDescendent(pathElements, source, requireForWrite);
    }

    private static final int DEPTH_LIMIT = 10;

    private void addDescendent(LinkedList<String> pathElements, XMLReader source, boolean requireForWrite) {
        String subName = pathElements.removeFirst();
        IntegratorOutputNode subNode;
        int index = -1;
        if ((index = names.indexOf(subName)) != -1) {
            if (pathElements.isEmpty()) {
                throw new RuntimeException("duplicate element path specification: "+subName);
            }
            subNode = (IntegratorOutputNode) nodes.get(index);
        } else {
            if (pathElements.isEmpty()) {
                StatefulXMLFilter sxf = null;
                if (source != null) {
                    if (source instanceof StatefulXMLFilter) {
                        sxf = (StatefulXMLFilter) source;
                    } else {
                        sxf = new StatefulXMLFilter(DEPTH_LIMIT);
                        sxf.setParent(source);
                    }
                }
                subNode = new IntegratorOutputNode(sxf);
                requires.add(requireForWrite);
            } else {
                subNode = new IntegratorOutputNode(null);
                requires.add(false); // Otherwise add node manually (explicitly).
            }
            names.add(subName);
            nodes.add(subNode);
        }
        if (!pathElements.isEmpty()) {
            subNode.addDescendent(pathElements, source, requireForWrite);
        }
    }

    public void setAggregating(boolean aggregating) {
        this.aggregating = aggregating;
    }

    public Boolean isAggregating() {
        return aggregating;
    }

    @Override
    public void setName(String name) {
        this.name = name;
        if (inputFilter != null) {
            inputFilter.setName(name);
        }
    }

    @Override
    public String getName() {
        return name;
    }

    private static final Map<String,Boolean> unmodifiableFeatures;

    static {
        Map<String, Boolean> tmpFeatures = new HashMap<String, Boolean>();
        tmpFeatures.put("http://xml.org/sax/features/namespaces", true);
        tmpFeatures.put("http://xml.org/sax/features/namespace-prefixes", false);
        tmpFeatures.put("http://xml.org/sax/features/validation", false);
        tmpFeatures.put("http://xml.org/sax/features/string-interning", true); // XXX Make all of these reflect parent filters!!!
        unmodifiableFeatures = Collections.unmodifiableMap(tmpFeatures);
    }

    @Override
    public boolean getFeature(String name) throws SAXNotRecognizedException, SAXNotSupportedException {
        if (unmodifiableFeatures.containsKey(name)) {
            return unmodifiableFeatures.get(name);
        } else {
            throw new UnsupportedOperationException("getFeature("+name+")");
        }
    }

    @Override
    public void setFeature(String name, boolean value) throws SAXNotRecognizedException, SAXNotSupportedException {
        if (unmodifiableFeatures.containsKey(name)) {
            if (value != unmodifiableFeatures.get(name)) {
                throw new UnsupportedOperationException("cannot set feature "+name+" to "+value);
            }
        } else {
            throw new UnsupportedOperationException("setFeature("+name+", "+value+")");
        }
    }

    @Override
    public Object getProperty(String name) throws SAXNotRecognizedException, SAXNotSupportedException {
        if ("http://xml.org/sax/properties/lexical-handler".equals(name)) {
            return lexicalHandler;
        } else {
            throw new UnsupportedOperationException("getProperty("+name+")");
        }
    }
    LexicalHandler lexicalHandler = null;

    @Override
    public void setProperty(String name, Object value) throws SAXNotRecognizedException, SAXNotSupportedException {
        if ("http://xml.org/sax/properties/lexical-handler".equals(name)) {
            lexicalHandler = (LexicalHandler)value;
        } else {
            throw new UnsupportedOperationException("setFeature("+name+", "+value+")");
        }
    }

    @Override
    public void setEntityResolver(EntityResolver resolver) {
        if (logger.isTraceEnabled()) {
            logger.trace("ignoring setEntityResolver("+resolver+")");
        }
    }

    @Override
    public EntityResolver getEntityResolver() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    private DTDHandler dtdHandler = null;

    @Override
    public void setDTDHandler(DTDHandler handler) {
        this.dtdHandler = handler;
    }

    @Override
    public DTDHandler getDTDHandler() {
        return dtdHandler;
    }

    @Override
    public ContentHandler getContentHandler() {
        return rawOutput;
    }

    private ErrorHandler errorHandler = null;

    @Override
    public void setErrorHandler(ErrorHandler handler) {
        this.errorHandler = handler;
    }

    @Override
    public ErrorHandler getErrorHandler() {
        return errorHandler;
    }

    @Override
    public void parse(InputSource input) throws IOException, SAXException {
        run();
    }

    @Override
    public void parse(String systemId) throws IOException, SAXException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setContentHandler(ContentHandler ch) {
        assignOutput(ch);
    }

    private void assignOutput(ContentHandler ch) {
        rawOutput = ch;
        if (dumpFile != null) {
            DumpingContentHandler dumper = new DumpingContentHandler();
            dumper.setDumpFile(dumpFile);
            dumper.setContentHandler(ch);
            output = dumper;
        } else {
            output = ch;
        }
    }

    private Boolean aggregating = null;

    @Override
    public void run() {
        if (nodes.isEmpty()) {
            if (inputFilter == null) {
                throw new IllegalStateException();
            } else {
                synchronized (this) {
                    assignOutput(inputFilter);
                    notify();
                }
                inputFilter.run();
            }
        } else {
            if (output == null) {
                synchronized (this) {
                    StatefulXMLFilter sxf = new StatefulXMLFilter(DEPTH_LIMIT);
                    sxf.setName(name+"Output");
                    assignOutput(sxf);
                    notify();
                }
            }
            if (inputFilter != null) {
                names.addFirst(null);
                nodes.addFirst(inputFilter);
                requires.addFirst(true);
                if (aggregating == null) {
                    aggregating = false;
                    System.out.println(this+"assigned aggregating="+aggregating);
                }
            } else {
                if (aggregating == null) {
                    aggregating = nodes.size() == 1;
                    System.out.println(this+"assigned aggregating="+aggregating);
                }
            }
            int size = nodes.size();
            childNodes = nodes.toArray(new IdQueryable[size]);
            childElementNames = names.toArray(new String[size]);
            requireForWrite = requires.toArray(new Boolean[size]);

            for (int i = 0; i < childNodes.length; i++) {
                Thread t;
                if (childElementNames[i] != null) {
                    childNodes[i].setName(childElementNames[i]);
                    t = new Thread(childNodes[i], childElementNames[i]+"<-"+Thread.currentThread().getName());
                } else {
                    t = new Thread(childNodes[i], childNodes[i].getName()+"<-"+Thread.currentThread().getName());
                }
                t.setUncaughtExceptionHandler(interrupter);
                t.start();
                if (requireForWrite[i]) {
                    requiredIndexes.add(i);
                }
            }
            try {
                run2();
            } catch (SAXException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    private static final Thread.UncaughtExceptionHandler interrupter = new ThreadGroupInterrupter();

    private static class ThreadGroupInterrupter implements Thread.UncaughtExceptionHandler {

        @Override
        public void uncaughtException(Thread t, Throwable e) {
            e.printStackTrace(System.err);
            t.getThreadGroup().interrupt();
        }

    }

    private final LinkedHashSet<Integer> requiredIndexes = new LinkedHashSet<Integer>();

    public static void main(String[] args) throws ParserConfigurationException, SAXException, TransformerConfigurationException, TransformerException {
        IntegratorOutputNode root = new IntegratorOutputNode();
        root.addDescendent("/simpleItemEven", new PreConfiguredXMLReader(new InputSource("./src/test/resources/input/simpleItemEven.xml")), false);
        root.addDescendent("/simpleItemOdd", new PreConfiguredXMLReader(new InputSource("./src/test/resources/input/simpleItemOdd.xml")), false);
        SAXTransformerFactory tf = (SAXTransformerFactory) TransformerFactory.newInstance("net.sf.saxon.TransformerFactoryImpl", null);
        Transformer t = tf.newTransformer();
        t.setOutputProperty(OutputKeys.INDENT, "yes");
        one(t, root);
    }

    private static void one(Transformer t, IntegratorOutputNode root) throws TransformerException {
        t.transform(new SAXSource(root, new InputSource()), new StreamResult(System.out));
    }

    private void run2() throws SAXException {
        LinkedHashSet<Integer> activeIndexes = new LinkedHashSet<Integer>();
        Integer lastLevel = null;
        int level;
        Comparable leastId;
        int lastOutputIndex = -1;
        int dipLevel = 0;
        boolean requiredInputExhausted = false;
        boolean allFinished = false;
        boolean[] eligible = new boolean[childNodes.length];
        int[] levels = new int[eligible.length];
        Arrays.fill(eligible, true);
        while (!allFinished && !requiredInputExhausted) {
            level = -1;
            leastId = null;
            allFinished = true;
            for (int i = 0; i < levels.length; i++) {
                try {
                    levels[i] = childNodes[i].getLevel();
                    if (levels[i] > level && eligible[i]) {
                        level = levels[i];
                    }
                } catch (EOFException ex) {
                    levels[i] = -1;
                    if (requiredIndexes.contains(i)) {
                        requiredInputExhausted = true;
                    }
                }
            }
            if (level < 0) {
                for (int i = 0; i < levels.length; i++) {
                    if (!eligible[i] && levels[i] >= 0) {
                        eligible[i] = true;
                        if (levels[i] > level) {
                            level = levels[i];
                        }
                    }
                }
            }
            if (level < dipLevel) {
                dipLevel = level;
            }
            activeIndexes.clear();
            boolean leastIdExplicitlySet = false;
            for (int i = 0; i < childNodes.length; i++) {
                try {
                    if (levels[i] == level) {
                        Comparable tmpId = childNodes[i].getId();
                        if (!leastIdExplicitlySet || (leastId != null && tmpId != null && tmpId.compareTo(leastId) < 0)) {
                            leastIdExplicitlySet = true;
                            for (int j : activeIndexes) {
                                eligible[j] = false;
                            }
                            activeIndexes.clear();
                            eligible[i] = true;
                            activeIndexes.add(i);
                            leastId = tmpId;
                        } else if ((leastId == null && tmpId == null) || tmpId.compareTo(leastId) == 0) { //tmpId should never be null.
                            eligible[i] = true;
                            activeIndexes.add(i);
                        } else if (tmpId.compareTo(leastId) > 0) {
                            eligible[i] = false;
                        }
                    }
                    allFinished = false;
                } catch (EOFException ex) {
                    // ok.
                }
            }
            System.out.println("level="+level+", leastId="+leastId);
            for (int i = 0; i < childNodes.length; i++) {
                IntegratorOutputNode child = (IntegratorOutputNode) childNodes[i];
                try {
                    System.out.println("\t" + childElementNames[i] + ": " + child.getLevel() + ", " + child.getId()+", active="+activeIndexes.contains(i));
                } catch (EOFException ex) {
                    // ok
                }
            }
            if (!activeIndexes.containsAll(requiredIndexes)) {
                boolean setOthersToEligible = true;
                for (int i : activeIndexes) {
                    try {
                        try {
                            childNodes[i].skipOutput();
                            if (childNodes[i].getLevel() == levels[i]) {
                                setOthersToEligible = false;
                            }
                        } catch (EOFException ex) {
                            throw new RuntimeException(ex);
                        }
                    } catch (IllegalStateException ex) {
                            System.out.println("exception on "+name);
                        for (int j = 0; j < levels.length; j++) {
                            try {
                                System.out.println(childElementNames[j] + ", level=" + levels[j] + ", id=" + childNodes[j].getId()+", eligible="+eligible[j]);
                            } catch (EOFException ex1) {
                                throw new RuntimeException(ex1);
                            }
                        }
                        throw ex;
                    }
                }
                if (setOthersToEligible) {
                    for (int i = 0; i < eligible.length; i++) {
                        if (levels[i] == level) {
                            eligible[i] = true;
                        }
                    }
                }
                if (requiredInputExhausted) {
                    for (int i = 0; i < childNodes.length; i++) {
                        Comparable consumeId;
                        while (!childNodes[i].isFinished()) {
                            try {
                                consumeId = childNodes[i].getId();
                                logger.trace(childElementNames[i]+ " consuming id: "+consumeId);
                                childNodes[i].skipOutput();
                                while (!childNodes[i].isFinished() && !childNodes[i].self()) {
                                    consumeId = childNodes[i].getId();
                                    logger.trace(childElementNames[i]+ " consuming id: "+consumeId);
                                    childNodes[i].step();
                                }
                            } catch (EOFException ex) {
                                logger.trace(childElementNames[i] + " finished consuming content");
                            }
                        }
                    }
                }
            } else {
                Boolean self = null;
                for (int i : activeIndexes) {
                    if (!childNodes[i].isFinished()) {
                        if (childNodes[i].self()) {
                            if (self == null) {
                                if (lastOutputIndex >= 0) {
                                    childNodes[lastOutputIndex].writeEndElements(output, dipLevel, aggregating);
                                }
                                childNodes[i].writeStartElements(output, dipLevel, aggregating);
                                self = true;
                                if (lastLevel == null || level > lastLevel) {
                                    lastLevel = level;
                                } else if (level < lastLevel) {
                                    throw new IllegalStateException("level=" + level + ", lastLevel=" + lastLevel);
                                }
                            } else if (!self) {
                                try {
                                    throw new IllegalStateException(name + childElementNames[i]+", "+childNodes[i].getId()+", childLevel="+childNodes[i].getLevel());
                                } catch (EOFException ex) {
                                    throw new RuntimeException(ex);
                                }
                            }
                            if (lastLevel == null || level >= lastLevel) {
                                if (childElementNames[i] != null) {
                                    attRunner.clear();
                                    if (leastId != null) {
                                        attRunner.addAttribute("", "id", "id", "CDATA", leastId.toString());
                                    }
                                    output.startElement("", childElementNames[i], childElementNames[i], attRunner);
                                }
                                childNodes[i].writeOutput(output);
                                if (childElementNames[i] != null) {
                                    output.endElement("", childElementNames[i], childElementNames[i]);
                                }
                                lastOutputIndex = i;
                                dipLevel = level; // selfLevel (highest)
                            }
                        } else {
                            if (self == null) {
                                self = false;
                                if (lastLevel == null || level > lastLevel) {
                                    lastLevel = level;
                                } else if (level < lastLevel) {
                                    lastLevel = level;
                                } else {
                                    if (level > 0 || leastId != null) {
                                        throw new IllegalStateException("non-self levels should not repeat; level=" + level + ", id=" + leastId+", lastLevel="+lastLevel);
                                    }
                                }
                            } else if (self) {
                                throw new IllegalStateException("inconsistent child selfness for "+childElementNames[i]+", asserted level=" + level + ", id=" + leastId);
                            }
                            if (!childNodes[i].isFinished()) {
                                childNodes[i].step();
                            }
                        }
                    }
                }
            }
        }
        if (lastOutputIndex < 0) {
            childNodes[0].writeRootElement(output);
        } else {
            childNodes[lastOutputIndex].writeEndElements(output, 0, aggregating);
        }
    }

    @Override
    public void step() {
        if (!debugging || rawOutput instanceof IdQueryable) {
            ((IdQueryable)rawOutput).step();
        }
    }

    public IntegratorOutputNode() {
        this.inputFilter = null;
    }

    public IntegratorOutputNode(StatefulXMLFilter payload) {
        if (payload != null) {
            this.inputFilter = payload;
        } else {
            this.inputFilter = null;
        }
    }

    @Override
    public boolean isFinished() {
        if (!debugging || rawOutput instanceof IdQueryable) {
            return ((IdQueryable)rawOutput).isFinished();
        } else {
            throw new IllegalStateException();
        }
    }

    @Override
    public void skipOutput() {
        if (!debugging || rawOutput instanceof IdQueryable) {
            ((IdQueryable)rawOutput).skipOutput();
        }
    }

    @Override
    public void writeOutput(ContentHandler ch) {
        if (!debugging || rawOutput instanceof IdQueryable) {
            ((IdQueryable)rawOutput).writeOutput(ch);
        }
    }

    public String buffersToString() {
        return ((StatefulXMLFilter) rawOutput).buffersToString(DEPTH_LIMIT);
    }

//    @Override
//    public void writeOuterStartElement(ContentHandler ch, boolean asSelf) {
//        if (!debugging || rawOutput instanceof IdQueryable) {
//            ((IdQueryable)rawOutput).writeOuterStartElement(ch, asSelf);
//        }
//    }
//
//    @Override
//    public void writeInnerStartElement(ContentHandler ch) {
//        if (!debugging || rawOutput instanceof IdQueryable) {
//            ((IdQueryable)rawOutput).writeInnerStartElement(ch);
//        }
//    }
//
//    @Override
//    public void writeInnerEndElement(ContentHandler ch) {
//        if (!debugging || rawOutput instanceof IdQueryable) {
//            ((IdQueryable)rawOutput).writeInnerEndElement(ch);
//        }
//    }
//
//    @Override
//    public void writeOuterEndElement(ContentHandler ch) {
//        if (!debugging || rawOutput instanceof IdQueryable) {
//            ((IdQueryable)rawOutput).writeOuterEndElement(ch);
//        }
//    }
//
    @Override
    public void writeEndElements(ContentHandler ch, int lowerLevel, boolean aggregate) throws SAXException {
        if (!debugging || rawOutput instanceof IdQueryable) {
            ((IdQueryable)rawOutput).writeEndElements(ch, lowerLevel, aggregate);
        }
    }

    @Override
    public void writeStartElements(ContentHandler ch, int lowerLevel, boolean aggregate) throws SAXException {
        if (!debugging || rawOutput instanceof IdQueryable) {
            ((IdQueryable)rawOutput).writeStartElements(ch, lowerLevel, aggregate);
        }
    }

    @Override
    public void writeRootElement(ContentHandler ch) throws SAXException {
        if (!debugging || rawOutput instanceof IdQueryable) {
            ((IdQueryable)rawOutput).writeRootElement(ch);
        }
    }

    private AttributesImpl attRunner = new AttributesImpl();

    @Override
    public boolean self() {
        blockForOutputFilterInitialization();
        if (!debugging || rawOutput instanceof IdQueryable) {
            return ((IdQueryable)rawOutput).self();
        } else {
            throw new IllegalStateException();
        }
    }

    @Override
    public Comparable getId() throws EOFException {
        blockForOutputFilterInitialization();
        if (!debugging || rawOutput instanceof IdQueryable) {
            return ((IdQueryable)rawOutput).getId();
        } else {
            throw new IllegalStateException();
        }
    }

    @Override
    public int getLevel() throws EOFException {
        blockForOutputFilterInitialization();
        if (!debugging || rawOutput instanceof IdQueryable) {
            return ((IdQueryable)rawOutput).getLevel();
        } else {
            throw new IllegalStateException();
        }
    }

    private void blockForOutputFilterInitialization() {
        synchronized (this) {
            while (output == null) {
                notify();
                try {
                    wait();
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
            }
        }
    }

    public void addChild(String childElementName, IdQueryable child, boolean requireForWrite) {
        if (childElementName == null || child == null) {
            throw new NullPointerException();
        }
        names.add(childElementName);
        child.setName(childElementName);
        nodes.add(child);
        requires.add(requireForWrite);
    }

    private final LinkedList<String> names = new LinkedList<String>();
    private final LinkedList<IdQueryable> nodes = new LinkedList<IdQueryable>();
    private final LinkedList<Boolean> requires = new LinkedList<Boolean>();


    private static class PreConfiguredXMLReader extends XMLFilterImpl {

        private final InputSource in;

        private PreConfiguredXMLReader(InputSource in) throws ParserConfigurationException, SAXException {
            this.in = in;
            SAXParserFactory spf = SAXParserFactory.newInstance();
            spf.setNamespaceAware(true);
            SAXParser sp = spf.newSAXParser();
            setParent(sp.getXMLReader());
        }

        @Override
        public void parse(InputSource input) throws SAXException, IOException {
            super.parse(in);
        }

        @Override
        public void parse(String systemId) throws SAXException, IOException {
            super.parse(in);
        }

    }

}
