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

    private static final int DEFAULT_START_INDEX = 0;
    private static final int DEFAULT_SUFFIX_SIZE = 5;
    private static final int DEFAULT_RECORD_LEVEL = 1;
    private static final int DEFAULT_CHUNK_SIZE = 100;
    private int chunkSize = DEFAULT_CHUNK_SIZE;
    private volatile boolean parsing = false;
    private volatile Throwable producerThrowable;
    private FutureTask<?> producerTask;
    private int level = -1;
    private int recordStartEvent = -1;
    private int recordCount = 0;
    private final Lock parseLock = new ReentrantLock();
    private final Condition parseContinue = parseLock.newCondition();
    private final Condition parseChunkDone = parseLock.newCondition();
    private final ArrayDeque<StructuralStartEvent> startEventStack = new ArrayDeque<StructuralStartEvent>();
    private ExecutorService executor;

    public static void main(String[] args) throws ParserConfigurationException, SAXException, TransformerException, FileNotFoundException, IOException {
        System.out.println(String.format("%01d", 6));
        System.exit(0);
        TransformerFactory tf = TransformerFactory.newInstance("net.sf.saxon.TransformerFactoryImpl", null);
        Transformer t = tf.newTransformer();
        SplittingXMLFilter sxf = new SplittingXMLFilter();
        SAXParserFactory spf = SAXParserFactory.newInstance();
        spf.setNamespaceAware(true);
        SAXParser sp = spf.newSAXParser();
        XMLReader xmlReader = sp.getXMLReader();
        sxf.setParent(xmlReader);
        sxf.setXMLReaderCallback(new MyXMLReaderCallback(0, t, "out/", ".xml"));
        File in = new File("blah.xml");
        sxf.setExecutor(Executors.newCachedThreadPool());
        try {
            sxf.parse(new InputSource(new FileReader(in)));
        } finally {
            sxf.getExecutor().shutdown();
        }
    }

    public SplittingXMLFilter() {
        initParser.setParent(this);
        repeatParser.setParent(this);
    }

    public ExecutorService getExecutor() {
        return executor;
    }

    public void setExecutor(ExecutorService executor) {
        this.executor = executor;
    }

    public int getChunkSize() {
        return chunkSize;
    }

    public void setChunkSize(int size) {
        chunkSize = size;
    }

    public static class MyXMLReaderCallback implements XMLReaderCallback {

        private final File staticFile;
        private final File parentFile;
        private final String namePrefix;
        private final String postSuffix;
        private final String suffixFormat;
        private final Transformer t;
        private int i;

        private static String getDefaultSuffixFormat(int suffixLength) {
            return suffixLength < 1 ? null : "%0"+suffixLength+'d';
        }

        public MyXMLReaderCallback(String prefix) throws TransformerConfigurationException {
            this(TransformerFactory.newInstance().newTransformer(), prefix);
        }

        public MyXMLReaderCallback(Transformer t, String prefix) {
            this(DEFAULT_START_INDEX, t, prefix);
        }

        public MyXMLReaderCallback(int start, Transformer t, String prefix) {
            this(start, t, prefix, "");
        }

        public MyXMLReaderCallback(int start, Transformer t, String prefix, String postSuffix) {
            this(start, t, DEFAULT_SUFFIX_SIZE, prefix, postSuffix);
        }

        public MyXMLReaderCallback(int start, Transformer t, int suffixSize, String prefix, String postSuffix) {
            this(start, t, getDefaultSuffixFormat(suffixSize), prefix, postSuffix);
        }

        public MyXMLReaderCallback(int start, Transformer t, String suffixFormat, String prefix, String postSuffix) {
            this(start, t, suffixFormat, new File(prefix), postSuffix);
        }

        public MyXMLReaderCallback(int start, Transformer t, int suffixLength, File prefix, String postSuffix) {
            this(start, t, getDefaultSuffixFormat(suffixLength), prefix, postSuffix);
        }

        public MyXMLReaderCallback(int start, Transformer t, String suffixFormat, File prefix, String postSuffix) {
            this.i = start;
            this.t = t;
            this.parentFile = prefix.getParentFile();
            this.namePrefix = prefix.getName();
            this.postSuffix = postSuffix;
            this.suffixFormat = suffixFormat;
            if (suffixFormat == null) {
                this.staticFile = new File(parentFile, namePrefix + postSuffix);
            } else {
                staticFile = null;
            }
        }

        @Override
        public void callback(XMLReader reader, InputSource input) throws SAXException, IOException {
            t.reset();
            File nextFile = staticFile != null ? staticFile : new File(parentFile, namePrefix + String.format(suffixFormat, i++) + postSuffix);
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

    private void reset(boolean cancel) {
        try {
            if (producerTask != null) {
                if (cancel) {
                    producerTask.cancel(true);
                } else {
                    producerTask.get();
                }
            }
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        } catch (ExecutionException ex) {
            throw new RuntimeException(ex);
        }
        parsing = false;
        producerTask = null;
        producerThrowable = null;
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
            reset(true);
        } else {
            super.setFeature(name, value);
        }
    }

    @Override
    public boolean getFeature(String name) throws SAXNotRecognizedException, SAXNotSupportedException {
        if (RESET_PROPERTY_NAME.equals(name)) {
            try {
                return super.getFeature(name) && producerTask == null;
            } catch (SAXException ex) {
                return producerTask == null;
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
            reset(false);
        } catch (Exception ex) {
            handleCallbackException(ex);
        }
    }

    private void handleCallbackException(Exception ex) throws SAXException, IOException {
        reset(true);
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

        private void initParse(InputSource input, String systemId) throws SAXException, IOException {
            SplittingXMLFilter.this.setContentHandler(this);
            ProducerParser producerParser = new ProducerParser(input, systemId);
            producerTask = new ProducerFutureTask(producerParser);
            executor.execute(producerTask);
            parseLock.lock();
            try {
                awaitParseChunkDone();
            } finally {
                parseLock.unlock();
            }
        }
    }

    private class ProducerFutureTask extends FutureTask {

        public ProducerFutureTask(Runnable runnable, Object result) {
            super(runnable, result);
        }

        public ProducerFutureTask(Callable callable) {
            super(callable);
        }

        @Override
        protected void setException(Throwable t) {
            super.setException(t);
            if (!isCancelled()) {
                producerThrowable = t;
                parseLock.lock();
                try {
                    parseChunkDone.signal();
                } finally {
                    parseLock.unlock();
                }
            }
        }

    }

    private final RepeatParser repeatParser = new RepeatParser();

    private void awaitParseChunkDone() throws SAXException, IOException {
        try {
            parseChunkDone.await();
            if (producerThrowable != null) {
                Throwable cause = producerThrowable;
                producerThrowable = null;
                if (cause instanceof SAXException) {
                    throw (SAXException) cause;
                } else if (cause instanceof IOException) {
                    throw (IOException) cause;
                } else {
                    throw new RuntimeException(cause);
                }
            }
        } catch (InterruptedException ex) {
            producerTask.cancel(true);
            throw new RuntimeException(ex);
        }
    }

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
            parseLock.lock();
            try {
                parseContinue.signal();
                awaitParseChunkDone();
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

    private class ProducerParser implements Callable<Void> {

        private final InputSource input;
        private final String systemId;

        private ProducerParser(InputSource input, String systemId) {
            this.input = input;
            this.systemId = systemId;
        }

        @Override
        public Void call() throws SAXException, IOException {
            if (input != null) {
                SplittingXMLFilter.super.parse(input);
            } else {
                SplittingXMLFilter.super.parse(systemId);
            }
            return null;
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
        parsing = false;
        parseLock.lock();
        try {
            parseChunkDone.signal();
        } finally {
            parseLock.unlock();
        }
    }

    private void recordStart() throws SAXException {
        if (++recordCount > DEFAULT_CHUNK_SIZE) {
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
