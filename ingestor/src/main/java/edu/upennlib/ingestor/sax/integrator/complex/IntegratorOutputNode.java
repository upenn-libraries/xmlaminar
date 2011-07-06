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
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package edu.upennlib.ingestor.sax.integrator.complex;

import java.io.EOFException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Random;
import org.apache.log4j.Appender;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.TTCCLayout;
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

/**
 *
 * @author michael
 */
public class IntegratorOutputNode implements IdQueryable, XMLReader {

    private final StatefulXMLFilter inputFilter;
    private ContentHandler output;
    private String[] childElementNames;
    private IdQueryable[] childNodes;
    private Boolean[] requireForWrite;
    private String name;

    @Override
    public void setName(String name) {
        this.name = name;
        if (inputFilter != null) {
            inputFilter.setName(name);
        }
    }

    @Override
    public boolean getFeature(String name) throws SAXNotRecognizedException, SAXNotSupportedException {
        if ("http://xml.org/sax/features/namespaces".equals(name)) {
            return true;
        } else {
            throw new UnsupportedOperationException("getFeature("+name+")");
        }
    }

    @Override
    public void setFeature(String name, boolean value) throws SAXNotRecognizedException, SAXNotSupportedException {
        if ("http://xml.org/sax/features/namespaces".equals(name)) {
            if (!value) {
                throw new UnsupportedOperationException("cannot set namespaces feature to false");
            }
        } else if ("http://xml.org/sax/features/namespace-prefixes".equals(name)) {
            logger.trace("ignoring setFeature("+name+", "+value+")");
        } else if ("http://xml.org/sax/features/validation".equals(name)) {
            logger.trace("ignoring setFeature("+name+", "+value+")");
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
            throw new UnsupportedOperationException("setEntityResolver("+resolver+")");
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
        return output;
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

    public static enum State {POTENTIALLY_WRITABLE, WRITABLE, NOT_WRITABLE}
    private State state = State.POTENTIALLY_WRITABLE;

    protected Logger logger = Logger.getLogger(getClass());

    @Override
    public void setContentHandler(ContentHandler ch) {
        output = ch;
    }

    private boolean aggregating = false;

    @Override
    public void run() {
        if (nodes.isEmpty()) {
            if (inputFilter == null) {
                throw new IllegalStateException();
            } else {
                synchronized(this) {
                    output = inputFilter;
                    notify();
                }
                //System.out.println("noop aggregating="+aggregating+" for "+name);
                inputFilter.run();
            }
        } else {
            if (output == null) {
                synchronized (this) {
                    output = new StatefulXMLFilter();
                    ((StatefulXMLFilter)output).setName(name);
                    notify();
                }
            }
            if (inputFilter != null) {
                names.addFirst(null);
                nodes.addFirst(inputFilter);
                requires.addFirst(true);
            } else {
                aggregating = nodes.size() == 1;
                //System.out.println("aggregating="+aggregating+" for "+name);
            }
            int size = nodes.size();
            childNodes = nodes.toArray(new IdQueryable[size]);
            childElementNames = names.toArray(new String[size]);
            requireForWrite = requires.toArray(new Boolean[size]);

            for (int i = 0; i < childNodes.length; i++) {
                Thread t;
                if (childElementNames[i] != null) {
                    t = new Thread(childNodes[i], childElementNames[i]);
                } else {
                    t = new Thread(childNodes[i]);
                }
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
    private final LinkedHashSet<Integer> requiredIndexes = new LinkedHashSet<Integer>();

    private String getStackTrace() {
        StringWriter sw = new StringWriter();
        new RuntimeException().printStackTrace(new PrintWriter(sw));
        return sw.toString();
    }

    private void run2() throws SAXException {
        LinkedHashSet<Integer> activeIndexes = new LinkedHashSet<Integer>();
        Integer lastLevel = null;
        int level;
        Comparable leastId;
        boolean allFinished = false;
        while (!allFinished) {
            level = -1;
            leastId = null;
            allFinished = true;
            for (int i = 0; i < childNodes.length; i++) {
                try {
                    int tmpLevel = childNodes[i].getLevel();
                    if (tmpLevel > level) {
                        activeIndexes.clear();
                        activeIndexes.add(i);
                        level = tmpLevel;
                        leastId = childNodes[i].getId();
                    } else if (tmpLevel == level) {
                        Comparable tmpId = childNodes[i].getId();
                        if (tmpId == null ^ leastId == null) {
                            throw new IllegalStateException("tmpLevel="+tmpLevel+", tmpId="+tmpId+", level="+level+", leastId="+leastId);
                        } else if (tmpId == null || tmpId.compareTo(leastId) == 0) {
                            activeIndexes.add(i);
                        } else if (tmpId.compareTo(leastId) < 0) {
                            activeIndexes.clear();
                            activeIndexes.add(i);
                            leastId = tmpId;
                        }
                    }
                    allFinished = false;
                } catch (EOFException ex) {
                    // ok.
                }
            }
            if (!activeIndexes.containsAll(requiredIndexes)) {
                for (int i : activeIndexes) {
                    childNodes[i].skipOutput();
                }
            } else {
                Boolean self = null;
                for (int i : activeIndexes) {
                    if (!childNodes[i].isFinished()) {
                        if (childNodes[i].self()) {
                            if (self == null) {
                                self = true;
                                if (lastLevel == null || level > lastLevel) {
                                    childNodes[i].writeOuterStartElement(output, aggregating);
                                    if (!aggregating) {
                                        childNodes[i].writeInnerStartElement(output);
                                    }
                                } else if (level < lastLevel) {
                                    if (!aggregating) {
                                        childNodes[i].writeInnerEndElement(output);
                                    }
                                    childNodes[i].writeOuterEndElement(output);
                                } else if (level == lastLevel) {
                                    if (!aggregating) {
                                        childNodes[i].writeInnerEndElement(output);
                                        childNodes[i].writeInnerStartElement(output);
                                    }
                                }
                            } else if (!self) {
                                throw new IllegalStateException();
                            }
                            if (lastLevel == null || level >= lastLevel) {
                                if (childElementNames[i] != null) {
                                    System.out.println(name+":open "+childElementNames[i]);
                                    output.startElement("", childElementNames[i], childElementNames[i], attRunner);
                                }
                                childNodes[i].writeOutput(output);
                                if (childElementNames[i] != null) {
                                    System.out.println(name+":close "+childElementNames[i]);
                                    output.endElement("", childElementNames[i], childElementNames[i]);
                                }
                            }
                        } else {
                            if (self == null) {
                                self = false;
                                if (lastLevel == null || level > lastLevel) {
                                    childNodes[i].writeOuterStartElement(output, false);
                                } else if (level < lastLevel) {
                                    childNodes[i].writeOuterEndElement(output);
                                } else {
                                    if (level > 0 || leastId != null) {
                                        //throw new IllegalStateException("non-self levels should not repeat; level=" + level + ", id=" + leastId);
                                    }
                                }
                            } else if (self) {
                                throw new IllegalStateException("inconsistent child selfness, asserted level=" + level + ", id=" + leastId);
                            }
                            if (!childNodes[i].isFinished()) {
                                childNodes[i].step();
                            }
                        }
                    }
                }
            }
            lastLevel = level;
        }
        System.out.println("started on "+name);
        childNodes[0].writeInnerEndElement(output);
        childNodes[0].writeOuterEndElement(output);
        System.out.println("finished on "+name);
    }

    @Override
    public void step() {
        if (output instanceof IdQueryable) {
            ((IdQueryable)output).step();
        }
    }

    public IntegratorOutputNode(StatefulXMLFilter payload) {
        Appender appender = new ConsoleAppender(new TTCCLayout(), "System.out");
        logger.addAppender(appender);
        logger.setLevel(Level.TRACE);
        if (payload != null) {
            this.inputFilter = payload;
        } else {
            this.inputFilter = null;
        }
    }

    @Override
    public boolean isFinished() {
        if (output instanceof IdQueryable) {
            return ((IdQueryable)output).isFinished();
        } else {
            throw new IllegalStateException();
        }
    }

    @Override
    public void skipOutput() {
        if (output instanceof IdQueryable) {
            ((IdQueryable)output).skipOutput();
        }
    }

    @Override
    public void writeOutput(ContentHandler ch) {
        if (output instanceof IdQueryable) {
            ((IdQueryable)output).writeOutput(ch);
        }
    }

    @Override
    public void writeOuterStartElement(ContentHandler ch, boolean asSelf) {
        if (output instanceof IdQueryable) {
            ((IdQueryable)output).writeOuterStartElement(ch, asSelf);
        }
    }

    @Override
    public void writeInnerStartElement(ContentHandler ch) {
        if (output instanceof IdQueryable) {
            ((IdQueryable)output).writeInnerStartElement(ch);
        }
    }

    @Override
    public void writeInnerEndElement(ContentHandler ch) {
        if (output instanceof IdQueryable) {
            ((IdQueryable)output).writeInnerEndElement(ch);
        }
    }

    @Override
    public void writeOuterEndElement(ContentHandler ch) {
        if (output instanceof IdQueryable) {
            ((IdQueryable)output).writeOuterEndElement(ch);
        }
    }

    private AttributesImpl attRunner = new AttributesImpl();

    @Override
    public boolean self() {
        blockForOutputFilterInitialization();
        if (output instanceof IdQueryable) {
            return ((IdQueryable)output).self();
        } else {
            throw new IllegalStateException();
        }
    }

    @Override
    public Comparable getId() throws EOFException {
        blockForOutputFilterInitialization();
        if (output instanceof IdQueryable) {
            return ((IdQueryable)output).getId();
        } else {
            throw new IllegalStateException();
        }
    }

    @Override
    public int getLevel() throws EOFException {
        blockForOutputFilterInitialization();
        if (output instanceof IdQueryable) {
            return ((IdQueryable)output).getLevel();
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

    private LinkedList<String> names = new LinkedList<String>();
    private LinkedList<IdQueryable> nodes = new LinkedList<IdQueryable>();
    private LinkedList<Boolean> requires = new LinkedList<Boolean>();
}
