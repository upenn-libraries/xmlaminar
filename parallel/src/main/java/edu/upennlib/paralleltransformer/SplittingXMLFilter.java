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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;
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
public class SplittingXMLFilter extends QueueSourceXMLFilter {

    private static final int DEFAULT_START_INDEX = 0;
    private static final int DEFAULT_SUFFIX_SIZE = 5;
    private static final int DEFAULT_RECORD_LEVEL = 1;
    private static final int DEFAULT_CHUNK_SIZE = 100;
    private int chunkSize = DEFAULT_CHUNK_SIZE;
    private volatile boolean parsing = false;
    private Future<?> consumerTask;
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
        sxf.setInputType(InputType.direct);
        sxf.setChunkSize(1);
        SAXParserFactory spf = SAXParserFactory.newInstance();
        spf.setNamespaceAware(true);
        SAXParser sp = spf.newSAXParser();
        XMLReader xmlReader = sp.getXMLReader();
        sxf.setParent(xmlReader);
        sxf.setXMLReaderCallback(new IncrementingFileCallback(0, t, "out/out-", ".xml"));
        File in = new File("blah.xml");
        InputSource inSource = new InputSource(new FileReader(in));
        inSource.setSystemId(in.getPath());
        sxf.setExecutor(Executors.newCachedThreadPool());
        try {
            sxf.parse(inSource);
        } finally {
            sxf.shutdown();
        }
    }

    public SplittingXMLFilter() {
        initParser.setParent(this);
        repeatParser.setParent(this);
    }

    public int getChunkSize() {
        return chunkSize;
    }

    public void setChunkSize(int size) {
        chunkSize = size;
    }

    @Override
    protected void initialParse(InputSource in) {
        setContentHandler(initParser);
        ParseLooper pl = new ParseLooper(in, null, Thread.currentThread());
        consumerTask = getExecutor().submit(pl);
    }

    @Override
    protected void repeatParse(InputSource in) {
        setContentHandler(repeatParser);
        ParseLooper pl = new ParseLooper(in, null, Thread.currentThread());
        consumerTask = getExecutor().submit(pl);
    }

    @Override
    protected void finished() throws SAXException {
        
    }

    public static class StaticFileCallback implements XMLReaderCallback {

        private final File staticFile;
        private final Transformer t;

        public StaticFileCallback(Transformer t, File staticFile) {
            this.staticFile = staticFile;
            this.t = t;
        }

        public StaticFileCallback(File staticFile) throws TransformerConfigurationException {
            this(TransformerFactory.newInstance().newTransformer(), staticFile);
        }

        @Override
        public void callback(XMLReader reader, InputSource input) throws SAXException, IOException {
            writeToFile(reader, input, staticFile, t);
        }

        @Override
        public void callback(XMLReader reader, String systemId) throws SAXException, IOException {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

    }

    public static class IncrementingFileCallback implements XMLReaderCallback {

        private final File parentFile;
        private final String namePrefix;
        private final String postSuffix;
        private final String suffixFormat;
        private final Transformer t;
        private int i;

        private static String getDefaultSuffixFormat(int suffixLength) {
            return "%0"+suffixLength+'d';
        }

        public IncrementingFileCallback(String prefix) throws TransformerConfigurationException {
            this(TransformerFactory.newInstance().newTransformer(), prefix);
        }

        public IncrementingFileCallback(Transformer t, String prefix) {
            this(DEFAULT_START_INDEX, t, prefix);
        }

        public IncrementingFileCallback(int start, Transformer t, String prefix) {
            this(start, t, prefix, "");
        }

        public IncrementingFileCallback(int start, Transformer t, String prefix, String postSuffix) {
            this(start, t, DEFAULT_SUFFIX_SIZE, prefix, postSuffix);
        }

        public IncrementingFileCallback(int start, Transformer t, int suffixSize, String prefix, String postSuffix) {
            this(start, t, getDefaultSuffixFormat(suffixSize), prefix, postSuffix);
        }

        public IncrementingFileCallback(int start, Transformer t, String suffixFormat, String prefix, String postSuffix) {
            this(start, t, suffixFormat, new File(prefix), postSuffix);
        }

        public IncrementingFileCallback(int start, Transformer t, int suffixLength, File prefix, String postSuffix) {
            this(start, t, getDefaultSuffixFormat(suffixLength), prefix, postSuffix);
        }

        public IncrementingFileCallback(int start, Transformer t, String suffixFormat, File prefix, String postSuffix) {
            this.i = start;
            this.t = t;
            this.parentFile = prefix.getParentFile();
            this.namePrefix = prefix.getName();
            this.postSuffix = postSuffix == null ? "" : postSuffix;
            this.suffixFormat = suffixFormat;
        }

        @Override
        public void callback(XMLReader reader, InputSource input) throws SAXException, IOException {
            File nextFile = new File(parentFile, namePrefix + String.format(suffixFormat, i++) + postSuffix);
            writeToFile(reader, input, nextFile, t);
        }

        @Override
        public void callback(XMLReader reader, String systemId) throws SAXException, IOException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

    }

    private static void writeToFile(XMLReader reader, InputSource input, File nextFile, Transformer t) throws FileNotFoundException, IOException {
        t.reset();
        OutputStream out = new BufferedOutputStream(new FileOutputStream(nextFile));
        StreamResult res = new StreamResult(out);
        res.setSystemId(nextFile);
        try {
            t.transform(new SAXSource(reader, input), res);
        } catch (TransformerException ex) {
            throw new RuntimeException(ex);
        } finally {
            out.close();
        }
    }

    public void reset() {
        try {
            setFeature(RESET_PROPERTY_NAME, true);
        } catch (SAXException ex) {
            throw new AssertionError(ex);
        }
    }

    private void reset(boolean cancel) {
        try {
            if (consumerTask != null) {
                if (cancel) {
                    consumerTask.cancel(true);
                } else {
                    consumerTask.get();
                }
            }
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        } catch (ExecutionException ex) {
            throw new RuntimeException(ex);
        }
        parsing = false;
        consumerTask = null;
        consumerThrowable = null;
        producerThrowable = null;
        recordCount = 0;
        level = -1;
        recordStartEvent = -1;
        startEventStack.clear();
    }

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
            reset(true);
        } else {
            super.setFeature(name, value);
        }
    }

    @Override
    public boolean getFeature(String name) throws SAXNotRecognizedException, SAXNotSupportedException {
        if (RESET_PROPERTY_NAME.equals(name)) {
            try {
                return super.getFeature(name) && consumerTask == null;
            } catch (SAXException ex) {
                return consumerTask == null;
            }
        } else {
            return super.getFeature(name);
        }
    }

    private final InitParser initParser = new InitParser();

    private class InitParser extends XMLFilterImpl {
        @Override
        public void parse(InputSource input) throws SAXException, IOException {
            parse(input, null);
        }

        @Override
        public void parse(String systemId) throws SAXException, IOException {
            parse(null, systemId);
        }

        private void parse(InputSource input, String systemId) throws SAXException, IOException {
            parseLock.lock();
            try {
                System.out.println("pre");
                parseChunkDone.await();
                System.out.println("post");
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            } finally {
                parseLock.unlock();
            }
        }
    }

    private final RepeatParser repeatParser = new RepeatParser();

    private class RepeatParser extends XMLFilterImpl {

        @Override
        public void parse(InputSource input) throws SAXException, IOException {
            parse();
        }

        @Override
        public void parse(String systemId) throws SAXException, IOException {
            parse();
        }

        private void parse() throws SAXException, IOException {
            parseLock.lock();
            try {
                parseContinue.signal();
                parseChunkDone.await();
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            } finally {
                parseLock.unlock();
            }
        }

    }
    
    private class ParseLooper implements Runnable {

        private final InputSource input;
        private final String systemId;
        private final Thread producer;
        
        private ParseLooper(InputSource in, String systemId, Thread producer) {
            this.input = in;
            this.systemId = systemId;
            this.producer = producer;
        }
        
        private void parseLoop(InputSource input, String systemId) throws SAXException, IOException {
            parsing = true;
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
            reset(false);
        }

        @Override
        public void run() {
            try {
                parseLoop(input, systemId);
            } catch (Throwable t) {
                if (producerThrowable == null) {
                    consumerThrowable = t;
                    producer.interrupt();
                } else {
                    t = producerThrowable;
                    producerThrowable = null;
                    throw new RuntimeException(t);
                }
            }
        }

    }

    @Override
    public void parse(InputSource input) throws SAXException, IOException {
        parse(input, null);
    }

    @Override
    public void parse(String systemId) throws SAXException, IOException {
        parse(null, systemId);
    }
    
    private void parse(InputSource input, String systemId) {
        try {
            if (input != null) {
                super.parse(input);
            } else {
                super.parse(systemId);
            }
        } catch (Throwable t) {
            if (consumerThrowable == null) {
                producerThrowable = t;
                reset(true);
                consumerTask.cancel(true);
                throw new RuntimeException(t);
            } else {
                t = consumerThrowable;
                consumerThrowable = null;
                throw new RuntimeException(t);
            }
        }
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

    private class ProducerParser implements Runnable {

        private final InputSource input;
        private final String systemId;
        private final Thread consumer;

        private ProducerParser(InputSource input, String systemId, Thread consumer) {
            this.input = input;
            this.systemId = systemId;
            this.consumer = consumer;
        }

        @Override
        public void run() {
            try {
                if (input != null) {
                    SplittingXMLFilter.super.parse(input);
                } else {
                    SplittingXMLFilter.super.parse(systemId);
                }
            } catch (Throwable t) {
                if (producerThrowable == null) {
                    consumerThrowable = t;
                    consumer.interrupt();
                } else {
                    t = producerThrowable;
                    producerThrowable = null;
                    throw new RuntimeException(t);
                }
            }
        }
    }
    
    private volatile Throwable consumerThrowable;
    private volatile Throwable producerThrowable;

    @Override
    public void startDocument() throws SAXException {
        System.out.println(getContentHandler()+", "+super.getContentHandler());
        startEventStack.push(new StructuralStartEvent());
        super.startDocument();
    }

    @Override
    public void endDocument() throws SAXException {
        super.endDocument();
        startEventStack.pop();
        parsing = false;
        parseLock.lock();
        try {
            parseChunkDone.signal();
        } finally {
            parseLock.unlock();
        }
    }

    private void recordStart() throws SAXException {
        if (++recordCount > chunkSize) {
            writeSyntheticEndEvents();
            recordCount = 1; // the record we just entered!
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

    @Override
    public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
        if (++level == DEFAULT_RECORD_LEVEL && recordStartEvent++ < 0) {
            recordStart();
        } else if (level < DEFAULT_RECORD_LEVEL) {
            startEventStack.push(new StructuralStartEvent(uri, localName, qName, atts));
        }
        super.startElement(uri, localName, qName, atts);
    }

    @Override
    public void startPrefixMapping(String prefix, String uri) throws SAXException {
        if (level < DEFAULT_RECORD_LEVEL) {
            startEventStack.push(new StructuralStartEvent(prefix, uri));
        } else if (level == DEFAULT_RECORD_LEVEL && recordStartEvent++ < 0) {
            recordStart();
        }
        super.startPrefixMapping(prefix, uri);
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        super.endElement(uri, localName, qName);
        if (level < DEFAULT_RECORD_LEVEL) {
            startEventStack.pop();
        } else if (--level == DEFAULT_RECORD_LEVEL && --recordStartEvent < 0) {
            recordEnd();
        }
    }

    @Override
    public void endPrefixMapping(String prefix) throws SAXException {
        if (level < DEFAULT_RECORD_LEVEL) {
            startEventStack.pop();
        } else if (level == DEFAULT_RECORD_LEVEL && --recordStartEvent < 0) {
            recordEnd();
        }
        super.endPrefixMapping(prefix);
    }
}
