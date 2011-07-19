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

package edu.upennlib.ingestor.sax.integrator;

import edu.upennlib.ingestor.sax.utils.StartElementExtension;
import java.io.EOFException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import org.apache.log4j.Appender;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.TTCCLayout;
import org.xml.sax.Attributes;
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

    private final boolean debugging = true;
    private final StatefulXMLFilter inputFilter;
    private ContentHandler output;
    private String[] childElementNames;
    private IdQueryable[] childNodes;
    private Boolean outputImplementsStartElementExtension = null;
    private Boolean[] requireForWrite;
    private String name;

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
        throw new UnsupportedOperationException("setEntityResolver(" + resolver + ")");
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

    protected Logger logger = Logger.getLogger(getClass());

    @Override
    public void setContentHandler(ContentHandler ch) {
        output = ch;
        outputImplementsStartElementExtension = ch instanceof StartElementExtension;
    }

    private Boolean aggregating = null;

    @Override
    public void run() {
        if (nodes.isEmpty()) {
            if (inputFilter == null) {
                throw new IllegalStateException();
            } else {
                synchronized(this) {
                    setContentHandler(inputFilter);
                    notify();
                }
                inputFilter.run();
                }
        } else {
            if (output == null) {
                synchronized (this) {
                    StatefulXMLFilter localOut = new StatefulXMLFilter();
                    localOut.setName(name+"Output");
                    setContentHandler(localOut);
                    notify();
                }
            }
            if (inputFilter != null) {
                names.addFirst(null);
                nodes.addFirst(inputFilter);
                requires.addFirst(true);
                if (aggregating == null) {
                    aggregating = false;
                }
            } else {
                if (aggregating == null) {
                    aggregating = nodes.size() == 1;
                }
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
        boolean[] eligible = new boolean[childNodes.length];
        int[] levels = new int[eligible.length];
        Arrays.fill(eligible, true);
        while (!allFinished) {
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
                    // ok
                }
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
            } else {
                Boolean self = null;
                int firstIndexWritten = -1;
                for (int i : activeIndexes) {
                    if (!childNodes[i].isFinished()) {
                        if (childNodes[i].self()) {
                            if (self == null) {
                                self = true;
                                if (lastLevel == null || level >= lastLevel) {
                                    if (level > lastLevel) {
                                        childNodes[i].writeOuterStartElement(output, aggregating, outputImplementsStartElementExtension);
                                        lastLevel = level;
                                    }
                                    if (!aggregating) {
                                        childNodes[i].writeInnerStartElement(output, outputImplementsStartElementExtension);
                                    }
                                    firstIndexWritten = i;
                                } else {
                                    throw new IllegalStateException("level="+level+", lastLevel="+lastLevel);
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
                            }
                        } else {
                            if (self == null) {
                                self = false;
                                if (lastLevel == null || level > lastLevel) {
                                    childNodes[i].writeOuterStartElement(output, false, outputImplementsStartElementExtension);
                                    lastLevel = level;
                                } else if (level < lastLevel) {
                                    childNodes[i].writeOuterEndElement(output, outputImplementsStartElementExtension);
                                    lastLevel = level;
                                } else {
                                    if (level > 0 || leastId != null) {
                                        throw new IllegalStateException("non-self levels should not repeat; level=" + level + ", id=" + leastId);
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
                if (self != null && self && !aggregating) {
                    childNodes[firstIndexWritten].writeInnerEndElement(output, outputImplementsStartElementExtension);
                }
            }
        }
        if (!aggregating) {
            childNodes[0].writeInnerEndElement(output, outputImplementsStartElementExtension);
        }
        childNodes[0].writeOuterEndElement(output, outputImplementsStartElementExtension);
    }

    @Override
    public void step() {
        if (!debugging || output instanceof IdQueryable) {
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
        if (!debugging || output instanceof IdQueryable) {
            return ((IdQueryable)output).isFinished();
        } else {
            throw new IllegalStateException();
        }
    }

    @Override
    public void skipOutput() {
        if (!debugging || output instanceof IdQueryable) {
            ((IdQueryable)output).skipOutput();
        }
    }

    @Override
    public void writeOutput(ContentHandler ch) {
        if (!debugging || output instanceof IdQueryable) {
            ((IdQueryable)output).writeOutput(ch);
        }
    }

    @Override
    public void writeOuterStartElement(ContentHandler ch, boolean asSelf, boolean startElementExtension) {
        if (!debugging || output instanceof IdQueryable) {
            ((IdQueryable)output).writeOuterStartElement(ch, asSelf, startElementExtension);
        }
    }

    @Override
    public void writeInnerStartElement(ContentHandler ch, boolean startElementExtension) {
        if (!debugging || output instanceof IdQueryable) {
            ((IdQueryable)output).writeInnerStartElement(ch, startElementExtension);
        }
    }

    @Override
    public void writeInnerEndElement(ContentHandler ch, boolean startElementExtension) {
        if (!debugging || output instanceof IdQueryable) {
            ((IdQueryable)output).writeInnerEndElement(ch, startElementExtension);
        }
    }

    @Override
    public void writeOuterEndElement(ContentHandler ch, boolean startElementExtension) {
        if (!debugging || output instanceof IdQueryable) {
            ((IdQueryable)output).writeOuterEndElement(ch, startElementExtension);
        }
    }

    private AttributesImpl attRunner = new AttributesImpl();

    @Override
    public boolean self() {
        blockForOutputFilterInitialization();
        if (!debugging || output instanceof IdQueryable) {
            return ((IdQueryable)output).self();
        } else {
            throw new IllegalStateException();
        }
    }

    @Override
    public Comparable getId() throws EOFException {
        blockForOutputFilterInitialization();
        if (!debugging || output instanceof IdQueryable) {
            return ((IdQueryable)output).getId();
        } else {
            throw new IllegalStateException();
        }
    }

    @Override
    public int getLevel() throws EOFException {
        blockForOutputFilterInitialization();
        if (!debugging || output instanceof IdQueryable) {
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

    private final LinkedList<String> names = new LinkedList<String>();
    private final LinkedList<IdQueryable> nodes = new LinkedList<IdQueryable>();
    private final LinkedList<Boolean> requires = new LinkedList<Boolean>();

    @Override
    public void startElement(String uri, String localName, String qName, Attributes atts, Object... objectAtts) throws SAXException {
        throw new UnsupportedOperationException("Not supported.");
    }
}
