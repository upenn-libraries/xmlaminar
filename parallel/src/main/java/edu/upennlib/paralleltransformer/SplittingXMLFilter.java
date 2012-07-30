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

package edu.upennlib.paralleltransformer;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.stream.StreamResult;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXNotSupportedException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLFilterImpl;

/**
 *
 * @author Michael Gibney
 */
public class SplittingXMLFilter extends XMLFilterImpl {

    private static final int RECORD_LEVEL = 1;
    private static final int CHUNK_SIZE = 100;
    private volatile boolean parsing = false;
    private ProducerParser producerParser;
    private int level = -1;
    private int recordStartEvent = -1;
    private int recordCount = 0;
    private final Lock parseLock = new ReentrantLock();
    private final Condition parseContinue = parseLock.newCondition();
    private final Condition parseChunkDone = parseLock.newCondition();
    private final ArrayDeque<StructuralStartEvent> startEventStack = new ArrayDeque<StructuralStartEvent>();

    public static void main(String[] args) throws ParserConfigurationException, SAXException, TransformerException, FileNotFoundException, IOException {
        TransformerFactory tf = TransformerFactory.newInstance("net.sf.saxon.TransformerFactoryImpl", null);
        Transformer t = tf.newTransformer();
        SplittingXMLFilter sxf = new SplittingXMLFilter();
        SAXParserFactory spf = SAXParserFactory.newInstance();
        spf.setNamespaceAware(true);
        SAXParser sp = spf.newSAXParser();
        XMLReader xmlReader = sp.getXMLReader();
        System.out.println(sp+", "+xmlReader);
        sxf.setParent(xmlReader);
        sxf.setXMLReaderCallback(new MyXMLReaderCallback(0, t));
        File in = new File("franklin-small-dump.xml");
        sxf.parse(new InputSource(new FileReader(in)));
    }

    public SplittingXMLFilter() {
        initParser.setParent(this);
        repeatParser.setParent(this);
    }

    private static class MyXMLReaderCallback implements XMLReaderCallback {

        private final Transformer t;
        private int i;

        private MyXMLReaderCallback(int start, Transformer t) {
            this.i = start;
            this.t = t;
        }

        @Override
        public void callback(XMLReader reader, InputSource input) throws SAXException, IOException {
            t.reset();
            OutputStream out = new BufferedOutputStream(new FileOutputStream("out/" + String.format("%1$05d", i++) + ".xml"));
            try {
                t.transform(new SAXSource(reader, input), new StreamResult(out));
            } catch (TransformerException ex) {
                throw new RuntimeException(ex);
            }
            out.close();
        }

        @Override
        public void callback(XMLReader reader, String systemId) throws SAXException, IOException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

    }

    public void reset() {
        try {
            setFeature(RESET_PROPERTY_NAME, true);
        } catch (SAXException ex) {
            throw new AssertionError(ex);
        }
    }

    private void reset(boolean interruptDownstream) {
        if (producerParser != null) {
            producerParser.interrupt(interruptDownstream);
        }
        doReset();
    }

    private void doReset() {
        try {
            if (producerParser != null && producerParser != Thread.currentThread()) {
                producerParser.join();
            }
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
        parsing = false;
        producerParser = null;
        sourceException = null;
        recordCount = 0;
        level = -1;
        recordStartEvent = -1;
        startEventStack.clear();
    }

    public static final String RESET_PROPERTY_NAME = "http://xml.org/sax/features/reset";

    @Override
    public void setFeature(String name, boolean value) throws SAXNotRecognizedException, SAXNotSupportedException {
        if (RESET_PROPERTY_NAME.equals(name) && value) {
            try {
                super.setFeature(name, value);
            } catch (SAXException ex) {
                /*
                 * could run here if only want to run if deepest resettable parent.
                 * ... but I don't think that's the case here.
                 */
            }
            /*
             * reset(false) because the downstream consumer thread issued this
             * reset request (initiated the interruption).
             */
            reset(false);
        } else {
            super.setFeature(name, value);
        }
    }

    @Override
    public boolean getFeature(String name) throws SAXNotRecognizedException, SAXNotSupportedException {
        if (RESET_PROPERTY_NAME.equals(name)) {
            try {
                return super.getFeature(name) && producerParser == null;
            } catch (SAXException ex) {
                return producerParser == null;
            }
        } else {
            return super.getFeature(name);
        }
    }

    private void parseLoop(InputSource input, String systemId) throws SAXException, IOException {
        parsing = true;
        try {
            if (input != null) {
                xmlReaderCallback.callback(initParser, input);
                while (parsing) {
                    xmlReaderCallback.callback(repeatParser, input);
                }
            } else {
                xmlReaderCallback.callback(initParser, systemId);
                while (parsing) {
                    xmlReaderCallback.callback(repeatParser, systemId);
                }
            }
        } catch (Exception ex) {
            handleCallbackException(ex);
        }
    }

    private void handleCallbackException(Exception ex) throws SAXException, IOException {
        if (ex != sourceException) {
            reset(false);
        } else {
            doReset();
        }
        if (ex instanceof SAXException) {
            throw (SAXException) ex;
        } else if (ex instanceof IOException) {
            throw (IOException) ex;
        } else {
            throw (RuntimeException) ex;
        }
    }

    private final InitParser initParser = new InitParser();

    private class InitParser extends XMLFilterImpl {
        @Override
        public void parse(InputSource input) throws SAXException, IOException {
            initParse(input, null);
        }

        @Override
        public void parse(String systemId) throws SAXException, IOException {
            initParse(null, systemId);
        }

        private void initParse(InputSource input, String systemId) {
            SplittingXMLFilter.this.setContentHandler(this);
            producerParser = new ProducerParser(input, systemId);
            producerParser.start();
            parseLock.lock();
            try {
                try {
                    parseChunkDone.await();
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
            } finally {
                parseLock.unlock();
            }
        }
    }

    private final RepeatParser repeatParser = new RepeatParser();

    private class RepeatParser extends XMLFilterImpl {

        @Override
        public void parse(InputSource input) throws SAXException, IOException {
            repeatParse();
        }

        @Override
        public void parse(String systemId) throws SAXException, IOException {
            repeatParse();
        }

        private void repeatParse() throws SAXException, IOException {
            SplittingXMLFilter.this.setContentHandler(this);
            try {
                parseLock.lock();
                parseContinue.signal();
                try {
                    parseChunkDone.await();
                } catch (InterruptedException ex) {
                    if (sourceException != null) {
                        if (sourceException instanceof SAXException) {
                            throw new UpstreamSAXException(ex, ((SAXException) sourceException));
                        } else if (sourceException instanceof IOException) {
                            throw new IOException("upstream IOException", sourceException);
                        }
                    }
                    throw new RuntimeException(ex);
                }
            } finally {
                parseLock.unlock();
            }
        }

    }

    @Override
    public void parse(InputSource input) throws SAXException, IOException {
        parseLoop(input, null);
    }

    @Override
    public void parse(String systemId) throws SAXException, IOException {
        parseLoop(null, systemId);
    }

    public XMLReaderCallback getXMLReaderCallback() {
        return xmlReaderCallback;
    }

    public void setXMLReaderCallback(XMLReaderCallback xmlReaderCallback) {
        this.xmlReaderCallback = xmlReaderCallback;
    }

    private XMLReaderCallback xmlReaderCallback;

    public static interface XMLReaderCallback {
        void callback(XMLReader reader, InputSource input) throws SAXException, IOException;
        void callback(XMLReader reader, String systemId) throws SAXException, IOException;
    }

    private class ProducerParser extends Thread {

        private final InputSource input;
        private final String systemId;
        private final Thread consumer;
        private volatile boolean interruptConsumer = true;

        @Override
        public void interrupt() {
            interrupt(true);
        }

        public void interrupt(boolean interruptConsumer) {
            this.interruptConsumer = interruptConsumer;
            super.interrupt();
        }

        private ProducerParser(InputSource input, String systemId) {
            this.consumer = Thread.currentThread();
            this.input = input;
            this.systemId = systemId;
        }

        @Override
        public void run() {
            try {
                if (input != null) {
                    SplittingXMLFilter.super.parse(input);
                } else {
                    SplittingXMLFilter.super.parse(systemId);
                }
            } catch (Exception ex) {
                sourceException = ex;
                if (interruptConsumer) {
                    sourceException.printStackTrace(System.err);
                    consumer.interrupt();
                }
            }
        }

    }

    @Override
    public void startDocument() throws SAXException {
        startEventStack.push(new StructuralStartEvent());
        super.startDocument();
    }

    @Override
    public void endDocument() throws SAXException {
        super.endDocument();
        startEventStack.pop();
        parseLock.lock();
        try {
            parseChunkDone.signal();
        } finally {
            parseLock.unlock();
        }
        doReset();
    }

    private void recordStart() throws SAXException {
        if (++recordCount > CHUNK_SIZE) {
            writeSyntheticEndEvents();
            recordCount = 0;
            try {
                parseLock.lock();
                parseChunkDone.signal();
                try {
                    parseContinue.await();
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
            } finally {
                parseLock.unlock();
            }
            writeSyntheticStartEvents();
        }
    }

    private void writeSyntheticEndEvents() throws SAXException {
        Iterator<StructuralStartEvent> iter = startEventStack.iterator();
        while (iter.hasNext()) {
            StructuralStartEvent next = iter.next();
            switch (next.type) {
                case DOCUMENT:
                    super.endDocument();
                    break;
                case PREFIX_MAPPING:
                    super.endPrefixMapping(next.one);
                    break;
                case ELEMENT:
                    super.endElement(next.one, next.two, next.three);
            }
        }
    }

    private void writeSyntheticStartEvents() throws SAXException {
        Iterator<StructuralStartEvent> iter = startEventStack.descendingIterator();
        while (iter.hasNext()) {
            StructuralStartEvent next = iter.next();
            switch (next.type) {
                case DOCUMENT:
                    super.startDocument();
                    break;
                case PREFIX_MAPPING:
                    super.startPrefixMapping(next.one, next.two);
                    break;
                case ELEMENT:
                    super.startElement(next.one, next.two, next.three, next.atts);
            }
        }

    }

    private void recordEnd() {

    }

    private Throwable sourceException;

    @Override
    public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
        if (++level == RECORD_LEVEL && recordStartEvent++ < 0) {
            recordStart();
        } else if (level < RECORD_LEVEL) {
            startEventStack.push(new StructuralStartEvent(uri, localName, qName, atts));
        }
        super.startElement(uri, localName, qName, atts);
    }

    @Override
    public void startPrefixMapping(String prefix, String uri) throws SAXException {
        if (level < RECORD_LEVEL) {
            startEventStack.push(new StructuralStartEvent(prefix, uri));
        } else if (level == RECORD_LEVEL && recordStartEvent++ < 0) {
            recordStart();
        }
        super.startPrefixMapping(prefix, uri);
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        super.endElement(uri, localName, qName);
        if (level < RECORD_LEVEL) {
            startEventStack.pop();
        } else if (--level == RECORD_LEVEL && --recordStartEvent < 0) {
            recordEnd();
        }
    }

    @Override
    public void endPrefixMapping(String prefix) throws SAXException {
        if (level < RECORD_LEVEL) {
            startEventStack.pop();
        } else if (level == RECORD_LEVEL && --recordStartEvent < 0) {
            recordEnd();
        }
        super.endPrefixMapping(prefix);
    }
}
