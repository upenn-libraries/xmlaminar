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

package edu.upennlib.xmlutils;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.stream.StreamResult;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.Locator;
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
public class SplittingXMLFilter extends XMLFilterLexicalHandlerImpl {

    public final boolean ENFORCE_RECORD_LEVEL_ELEMENT_CONSISTENCY = true;
    boolean parsingInitiated = false;
    public final int DEFAULT_CHUNK_SIZE = 20;
    private int chunkSize = DEFAULT_CHUNK_SIZE;
    public static final boolean BUFFERING = true;
    public final int RECORD_LEVEL = 1;
    private String[] recordElementIdentifiers = null;
    private int workingRecordCount = 0;
    private int level = -1;
    private Boolean inRecord = null;
    private boolean encounteredFirstRecord = false;
    private MyEndEventStack endEventStack = new MyEndEventStack();
    private UnboundedContentHandlerBuffer startEvents = new UnboundedContentHandlerBuffer();

    public static void main(String[] args) throws FileNotFoundException, SAXException, IOException, ParserConfigurationException, TransformerConfigurationException, TransformerException {
        SplittingXMLFilter instance = new SplittingXMLFilter();
        SAXParserFactory spf = SAXParserFactory.newInstance();
        spf.setNamespaceAware(true);
        SAXParser parser = spf.newSAXParser();
        instance.setParent(parser.getXMLReader());
        BufferedInputStream bis = new BufferedInputStream(new FileInputStream("/tmp/large.xml"));
        InputSource inputSource = new InputSource(bis);
        Transformer t = TransformerFactory.newInstance().newTransformer();
        long start = System.currentTimeMillis();
        int nextFileNumber = 0;
        do {
            File file = new File("/tmp/splitOutput/testSplitOut" + String.format("%05d", nextFileNumber++) + ".xml");
            BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(file));
            t.transform(new SAXSource(instance, inputSource), new StreamResult(bos));
            bos.close();
        } while (instance.hasMoreOutput(inputSource));
        System.out.println("duration: "+(System.currentTimeMillis() - start));
    }

    public SplittingXMLFilter() {
        if (BUFFERING) {
            buffer = new BoundedXMLFilterBuffer();
        } else {
            buffer = null;
        }
    }

    public void setChunkSize(int chunkSize) {
        this.chunkSize = chunkSize;
    }

    private boolean checkRotate(String uri, String localName, String name, Attributes atts) {
        if (level != RECORD_LEVEL) {
            return false;
        } else {
            encounteredFirstRecord = true;
            endEventStack.lock();
            if (ENFORCE_RECORD_LEVEL_ELEMENT_CONSISTENCY) {
                if (recordElementIdentifiers == null) {
                    if (level == RECORD_LEVEL) {
                        recordElementIdentifiers = new String[3];
                        recordElementIdentifiers[0] = uri;
                        recordElementIdentifiers[1] = localName;
                        recordElementIdentifiers[2] = name;
                    }
                }
                if (recordElementIdentifiers != null) {
                    if (stringIntern) {
                        if (recordElementIdentifiers[0] == uri && recordElementIdentifiers[1] == localName && recordElementIdentifiers[2] == name) {
                            inRecord = true;
                        } else {
                            throw new IllegalStateException("record level element consistency check failed"+recordElementIdentifiers[2]+"?="+name);
                        }
                    } else {
                        if (recordElementIdentifiers[0].equals(uri) && recordElementIdentifiers[1].equals(localName) && recordElementIdentifiers[2].equals(name)) {
                            inRecord = true;
                        } else {
                            throw new IllegalStateException("record level element consistency check failed"+recordElementIdentifiers[2]+"?="+name);
                        }
                    }
                }
            }
            return workingRecordCount >= chunkSize;
        }
    }

    private void writeSyntheticEndEvents() {
        try {
            level += endEventStack.writeEndEvents(getContentHandler());
        } catch (SAXException ex) {
            throw new RuntimeException(ex);
        }
        try {
            parseLock.lock();
            parsePaused.signal();
            try {
                parseCalled.await();
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            }
        } finally {
            parseLock.unlock();
        }
    }

    private void writeSyntheticStartEvents() {
        try {
            level += startEvents.play(getContentHandler(), lh);
        } catch (SAXException ex) {
            throw new RuntimeException(ex);
        }
        try {
            if (tmpStartAtts != null) {
                startElement(tmpStartStringArgs[0], tmpStartStringArgs[1], tmpStartStringArgs[2], tmpStartAtts);
            }
        } catch (SAXException ex) {
            throw new RuntimeException(ex);
        }
        tmpStartAtts = null;
    }

    public boolean hasMoreOutput(InputSource input) {
        return input == workingInputSource && hasMoreOutput;
    }

    public boolean hasMoreOutput(String systemId) {
        return systemId.equals(workingSystemId) && hasMoreOutput;
    }

    private final String[] tmpStartStringArgs = new String[3];
    private Attributes tmpStartAtts;
    
    @Override
    public void startElement(String uri, String localName, String name, Attributes atts) throws SAXException {
        level++;
        if (checkRotate(uri, localName, name, atts)) {
            tmpStartStringArgs[0] = uri;
            tmpStartStringArgs[1] = localName;
            tmpStartStringArgs[2] = name;
            tmpStartAtts = atts;
            writeSyntheticEndEvents();
        } else {
            Attributes myAtt = new AttributesImpl(atts);
            if (!encounteredFirstRecord) {
                startEvents.startElement(uri, localName, name, myAtt);
                endEventStack.startElement(uri, localName, name, myAtt);
            }
            super.startElement(uri, localName, name, atts);
        }
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        if (!encounteredFirstRecord) {
            startEvents.endElement(uri, localName, qName);
            endEventStack.endElement(uri, localName, qName);
        }
        super.endElement(uri, localName, qName);
        if (level-- == RECORD_LEVEL) {
            if (ENFORCE_RECORD_LEVEL_ELEMENT_CONSISTENCY) {
                if (!inRecord) {
                    throw new IllegalStateException();
                }
                inRecord = false;
            }
            workingRecordCount++;
        }
    }

    @Override
    public void endDocument() throws SAXException {
        if (!encounteredFirstRecord) {
            startEvents.endDocument();
            endEventStack.endDocument();
        }
        super.endDocument();
        hasMoreOutput = false;
        try {
            parseLock.lock();
            parsePaused.signal();
        } finally {
            parseLock.unlock();
        }
    }

    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        if (!encounteredFirstRecord) {
            startEvents.characters(ch, start, length);
        }
        super.characters(ch, start, length);
    }

    @Override
    public void endPrefixMapping(String prefix) throws SAXException {
        if (!encounteredFirstRecord) {
            startEvents.endPrefixMapping(prefix);
            endEventStack.endPrefixMapping(prefix);
        }
        super.endPrefixMapping(prefix);
    }

    @Override
    public void ignorableWhitespace(char[] ch, int start, int length) throws SAXException {
        if (!encounteredFirstRecord) {
            startEvents.ignorableWhitespace(ch, start, length);
        }
        super.ignorableWhitespace(ch, start, length);
    }

//    @Override
//    public void notationDecl(String name, String publicId, String systemId) throws SAXException {
//        if (!encounteredFirstRecord) {
//            startEvents.notationDecl(name, publicId, systemId);
//        }
//        super.notationDecl(name, publicId, systemId);
//    }
//
    @Override
    public void processingInstruction(String target, String data) throws SAXException {
        if (!encounteredFirstRecord) {
            startEvents.processingInstruction(target, data);
        }
        super.processingInstruction(target, data);
    }

//    @Override
//    public InputSource resolveEntity(String publicId, String systemId) throws SAXException, IOException {
//        if (!encounteredFirstRecord) {
//            startEvents.resolveEntity(publicId, systemId);
//        }
//        return super.resolveEntity(publicId, systemId);
//    }
//
    @Override
    public void setDocumentLocator(Locator locator) {
        if (!encounteredFirstRecord) {
            startEvents.setDocumentLocator(locator);
        }
        super.setDocumentLocator(locator);
    }

    @Override
    public void skippedEntity(String name) throws SAXException {
        if (!encounteredFirstRecord) {
            startEvents.skippedEntity(name);
        }
        super.skippedEntity(name);
    }

    @Override
    public void startDocument() throws SAXException {
        if (!encounteredFirstRecord) {
            startEvents.startDocument();
            endEventStack.startDocument();
        }
        super.startDocument();
    }

    @Override
    public void startPrefixMapping(String prefix, String uri) throws SAXException {
        if (!encounteredFirstRecord) {
            startEvents.startPrefixMapping(prefix, uri);
            endEventStack.startPrefixMapping(prefix, uri);
        }
        super.startPrefixMapping(prefix, uri);
    }

    @Override
    public void unparsedEntityDecl(String name, String publicId, String systemId, String notationName) throws SAXException {
        if (!encounteredFirstRecord) {
            startEvents.unparsedEntityDecl(name, publicId, systemId, notationName);
        }
        super.unparsedEntityDecl(name, publicId, systemId, notationName);
    }

    @Override
    public void comment(char[] ch, int start, int length) throws SAXException {
        if (!encounteredFirstRecord) {
            startEvents.comment(ch, start, length);
        }
        super.comment(ch, start, length);
    }

    @Override
    public void endCDATA() throws SAXException {
        if (!encounteredFirstRecord) {
            startEvents.endCDATA();
            endEventStack.endCDATA();
        }
        super.endCDATA();
    }

    @Override
    public void endDTD() throws SAXException {
        if (!encounteredFirstRecord) {
            startEvents.endDTD();
            endEventStack.endDTD();
        }
        super.endDTD();
    }

    @Override
    public void endEntity(String name) throws SAXException {
        if (!encounteredFirstRecord) {
            startEvents.endEntity(name);
            endEventStack.endEntity(name);
        }
        super.endEntity(name);
    }

    @Override
    public void startCDATA() throws SAXException {
        if (!encounteredFirstRecord) {
            startEvents.startCDATA();
            endEventStack.startCDATA();
        }
        super.startCDATA();
    }

    @Override
    public void startDTD(String name, String publicId, String systemId) throws SAXException {
        if (!encounteredFirstRecord) {
            startEvents.startDTD(name, publicId, systemId);
            endEventStack.startDTD(name, publicId, systemId);
        }
        super.startDTD(name, publicId, systemId);
    }

    @Override
    public void startEntity(String name) throws SAXException {
        if (!encounteredFirstRecord) {
            startEvents.startEntity(name);
            endEventStack.startEntity(name);
        }
        super.startEntity(name);
    }

    private final Lock parseLock = new ReentrantLock();
    private final Condition parseCalled = parseLock.newCondition();
    private final Condition parsePaused = parseLock.newCondition();

    @Override
    public void parse(InputSource input) throws SAXException, IOException {
        parse(input, null);
    }

    private boolean stringIntern = false;

    private void parse(InputSource input, String systemId) throws SAXNotRecognizedException, SAXNotSupportedException {
        workingRecordCount = 0;
        level = -1;
        if (hasMoreOutput && ((input != null && input == workingInputSource) || (systemId != null && systemId.equals(workingSystemId)))) {
            writeSyntheticStartEvents();
        } else {
            stringIntern = getFeature(SAXFeatures.STRING_INTERNING);
            workingInputSource = input;
            hasMoreOutput = true;
            if (BUFFERING) {
                buffer.clear();
                buffer.setParent(getParent());
                setParent(buffer);
            }
            XMLReader parent = getParent();
            parent.setContentHandler(this);
            parent.setProperty(LEXICAL_HANDLER_PROPERTY_KEY, this);
            parent.setDTDHandler(this);
            parent.setEntityResolver(this);
            parent.setErrorHandler(this);
            SuperParser sp;
            if (input != null) {
                sp = new SuperParser(parent, input);
            } else if (systemId != null) {
                sp = new SuperParser(parent, systemId);
            } else {
                throw new IllegalArgumentException();
            }
            if (superParserThread != null && superParserThread.isAlive()) {
                superParserThread.interrupt();
            }
            superParserThread = new Thread(sp, "superParser<-"+Thread.currentThread().getName());
            superParserThread.start();
        }
        try {
            parseLock.lock();
            parseCalled.signal();
            try {
                parsePaused.await();
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            }
        } finally {
            parseLock.unlock();
        }
    }

    private final BoundedXMLFilterBuffer buffer;

    private static class SuperParser implements Runnable {

        private final XMLReader superReader;
        private final InputSource inputSource;
        private final String systemId;

        public SuperParser(XMLReader superReader, InputSource inputSource) {
            this.superReader = superReader;
            this.inputSource = inputSource;
            systemId = null;
        }

        public SuperParser(XMLReader superReader, String systemId) {
            this.superReader = superReader;
            this.systemId = systemId;
            inputSource = null;
        }

        @Override
        public void run() {
            try {
                if (inputSource != null && systemId == null) {
                    superReader.parse(inputSource);
                } else if (systemId != null && inputSource == null) {
                    superReader.parse(systemId);
                } else {
                    throw new IllegalStateException();
                }
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            } catch (SAXException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    @Override
    public void parse(String systemId) throws SAXException, IOException {
        parse(null, systemId);
    }

    private InputSource workingInputSource;
    private Thread superParserThread;
    private String workingSystemId;
    private boolean hasMoreOutput;

    private static class MyEndEventStack implements ContentHandler, LexicalHandler {

        private LinkedList<Object[]> endEventStack = new LinkedList<Object[]>();
        private boolean locked = false;

        public int writeEndEvents(ContentHandler ch) throws SAXException {
            int level = 0;
            for (Object[] endEvent : endEventStack) {
                switch ((SaxEventType) endEvent[0]) {
                    case endElement:
                        ch.endElement((String) endEvent[1], (String) endEvent[2], (String) endEvent[3]);
                        level--;
                        break;
                    case endPrefixMapping:
                        ch.endPrefixMapping((String) endEvent[1]);
                        break;
                    case endDocument:
                        ch.endDocument();
                        break;
                    default:
                        throw new RuntimeException();
                }
            }
            return level;
        }

        public void lock() {
            locked = true;
        }

        public void clear() {
            endEventStack.clear();
            locked = false;
        }

        private void push(Object... args) {
            endEventStack.addFirst(args);
        }

        private void pop(Object... args) {
            Object[] remove = endEventStack.remove();
            for (int i = 0; i < remove.length; i++) {
                if (!remove[i].equals(args[i])) {
                    throw new IllegalStateException(Arrays.asList(args) + " !=  " + Arrays.asList(remove));
                }
            }
        }

        @Override
        public void startDocument() throws SAXException {
            if (!locked) {
                push(SaxEventType.endDocument);
            }
        }

        @Override
        public void startPrefixMapping(String prefix, String uri) throws SAXException {
            if (!locked) {
                push(SaxEventType.endPrefixMapping, prefix);
            }
        }

        @Override
        public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
            if (!locked) {
                push(SaxEventType.endElement, uri, localName, qName);
            }
        }

        @Override
        public void endDocument() throws SAXException {
            if (!locked) {
                pop(SaxEventType.endDocument);
            }
        }

        @Override
        public void endPrefixMapping(String prefix) throws SAXException {
            if (!locked) {
                pop(SaxEventType.endPrefixMapping, prefix);
            }
        }

        @Override
        public void endElement(String uri, String localName, String qName) throws SAXException {
            if (!locked) {
                pop(SaxEventType.endElement, uri, localName, qName);
            }
        }

        @Override
        public void characters(char[] ch, int start, int length) throws SAXException {
        }

        @Override
        public void ignorableWhitespace(char[] ch, int start, int length) throws SAXException {
        }

        @Override
        public void processingInstruction(String target, String data) throws SAXException {
        }

        @Override
        public void skippedEntity(String name) throws SAXException {
        }

        @Override
        public void setDocumentLocator(Locator locator) {
        }

        @Override
        public void startEntity(String name) {
            if (!locked) {
                push(SaxEventType.endEntity, name);
            }
        }

        @Override
        public void startDTD(String name, String publicId, String systemId) {
            if (!locked) {
                push(SaxEventType.endDTD);
            }
        }

        @Override
        public void startCDATA() {
            if (!locked) {
                push(SaxEventType.endCDATA);
            }
        }

        @Override
        public void endEntity(String name) {
            if (!locked) {
                pop(SaxEventType.endEntity, name);
            }
        }

        @Override
        public void endDTD() {
            if (!locked) {
                pop(SaxEventType.endDTD);
            }
        }

        @Override
        public void endCDATA() {
            if (!locked) {
                pop(SaxEventType.endCDATA);
            }
        }

        @Override
        public void comment(char[] ch, int start, int length) throws SAXException {
        }
    }
}
