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

import edu.upennlib.paralleltransformer.callback.IncrementingFileCallback;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Phaser;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.sax.SAXSource;
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

    private volatile boolean parsing = false;
    private volatile Future<?> consumerTask;
    private final Lock parseLock = new ReentrantLock();
    private final Condition parseContinue = parseLock.newCondition();
    private final Condition parseChunkDone = parseLock.newCondition();
    
    
    private int level = -1;
    private int startEventLevel = -1;
    private final ArrayDeque<StructuralStartEvent> startEventStack = new ArrayDeque<StructuralStartEvent>();

    public SplittingXMLFilter() {
        synchronousParser.setParent(this);
    }

    public static void main(String[] args) throws Exception {
        TransformerFactory tf = TransformerFactory.newInstance("net.sf.saxon.TransformerFactoryImpl", null);
        Transformer t = tf.newTransformer();
        SplittingXMLFilter sxf = new SplittingXMLFilter();
        sxf.setInputType(InputType.indirect);
        //sxf.setChunkSize(1);
        SAXParserFactory spf = SAXParserFactory.newInstance();
        spf.setNamespaceAware(true);
        SAXParser sp = spf.newSAXParser();
        XMLReader xmlReader = sp.getXMLReader();
        sxf.setParent(xmlReader);
        sxf.setXMLReaderCallback(new IncrementingFileCallback(0, t, "out/out-", ".xml"));
        File in = new File("blah.txt");
        InputSource inSource = new InputSource(new FileReader(in));
        inSource.setSystemId(in.getPath());
        sxf.setExecutor(Executors.newCachedThreadPool());
        try {
            sxf.parse(inSource);
        } finally {
            sxf.shutdown();
        }
    }
    
    @Override
    protected void initialParse(SAXSource in) {
        setupParse(in.getInputSource());
    }

    @Override
    protected void repeatParse(SAXSource in) {
        reset(false);
        setupParse(in.getInputSource());
    }

    private void setupParse(InputSource in) {
        setContentHandler(synchronousParser);
        ParseLooper pl = new ParseLooper(in, null, Thread.currentThread());
        consumerTask = getExecutor().submit(pl);
        parseLock.lock();
        try {
            parseContinue.await();
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        } finally {
            parseLock.unlock();
        }
    }

    @Override
    protected void finished() throws SAXException {
        reset(false);
    }

    public void reset() {
        try {
            setFeature(RESET_PROPERTY_NAME, true);
        } catch (SAXException ex) {
            throw new AssertionError(ex);
        }
    }

    protected void reset(boolean cancel) {
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
        if (splitDirector != null) {
            splitDirector.reset();
        }
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

    private final SynchronousParser synchronousParser = new SynchronousParser();

    private class SynchronousParser extends XMLFilterImpl {

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
                parseChunkDonePhaser.arrive();
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            } finally {
                parseLock.unlock();
            }
        }

        @Override
        public void endElement(String uri, String localName, String qName) throws SAXException {
            //System.out.println(getContentHandler()+".end("+localName+"), "+splitState);
            super.endElement(uri, localName, qName);
        }

        @Override
        public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
            //System.out.println(getContentHandler()+".start("+localName+"), "+splitState);
            super.startElement(uri, localName, qName, atts);
        }

        
    }
    
    private final Phaser parseChunkDonePhaser = new Phaser(2);
    
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
                while (parsing) {
                    xmlReaderCallback.callback(synchronousParser, input);
                    parseChunkDonePhaser.arriveAndAwaitAdvance();
                }
            } else {
                do {
                    xmlReaderCallback.callback(synchronousParser, systemId);
                } while (parsing);
            }
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
            xmlReaderCallback.finished();
        } catch (Throwable t) {
            if (consumerThrowable == null) {
                producerThrowable = t;
                reset(true);
                if (consumerTask != null) {
                    consumerTask.cancel(true);
                }
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
        void finished();
    }

    private volatile Throwable consumerThrowable;
    private volatile Throwable producerThrowable;

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

    private int splitLevel = Integer.MAX_VALUE;
    private int splitStartEventLevel = Integer.MAX_VALUE;
    
    protected final void split() throws SAXException {
        writeSyntheticEndEvents();
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

    private SplitDirector splitDirector = new SplitDirector();

    public SplitDirector getSplitDirector() {
        return splitDirector;
    }

    public void setSplitDirector(SplitDirector splitDirector) {
        this.splitDirector = splitDirector;
    }
    
    @Override
    public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
        level++;
        startEventLevel++;
        try {
            if (level < splitLevel && handleDirective(splitDirector.startElement(uri, localName, qName, atts, level))) {
            startEventStack.push(new StructuralStartEvent(uri, localName, qName, atts));
        }
        } catch (RuntimeException ex) {
            System.out.println("on qName="+qName);
            throw ex;
        }
        super.startElement(uri, localName, qName, atts);
    }

    @Override
    public void startPrefixMapping(String prefix, String uri) throws SAXException {
        startEventLevel++;
        if (level < splitLevel && handleDirective(splitDirector.startPrefixMapping(prefix, uri, level))) {
            startEventStack.push(new StructuralStartEvent(prefix, uri));
        }
        super.startPrefixMapping(prefix, uri);
    }

    
    
    /**
     * Returns true if the generating event should be added to 
     * or removed from the startEvent stack
     * @param directive
     * @return
     * @throws SAXException 
     */
    private boolean handleDirective(SplitDirective directive) throws SAXException {
            switch (directive) {
                case SPLIT:
                    splitLevel = level;
                    splitStartEventLevel = startEventLevel;
                case SPLIT_NO_BYPASS:
                    split();
                    break;
                case NO_SPLIT_BYPASS:
                    splitLevel = level;
                    splitStartEventLevel = startEventLevel;
                    return false;
                case NO_SPLIT:
                    return true;
            }
            return false;
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        super.endElement(uri, localName, qName);
        if (handleStructuralEndEvent()) {
            SplitDirective directive = splitDirector.endElement(uri, localName, qName, level);
            startEventLevel--;
            level--;
            handleDirective(directive);
        } else {
            startEventLevel--;
            level--;
        }
    }
    
    private boolean handleStructuralEndEvent() {
        if (level > splitLevel) {
            return false;
        } else if (level < splitLevel) {
            startEventStack.pop();
            return true;
        } else {
            if (startEventLevel == splitStartEventLevel) {
                bypassEnd();
                return true;
            } else if (startEventLevel < splitStartEventLevel) {
                startEventStack.pop();
                return true;
            } else {
                return false;
            }
        }
    }

    @Override
    public void endPrefixMapping(String prefix) throws SAXException {
        super.endPrefixMapping(prefix);
        if (handleStructuralEndEvent()) {
            SplitDirective directive = splitDirector.endPrefixMapping(prefix, level);
            startEventLevel--;
            handleDirective(directive);
        } else {
            startEventLevel--;
        }
    }
    
    private void bypassEnd() {
        splitLevel = Integer.MAX_VALUE;
        splitStartEventLevel = Integer.MAX_VALUE;
    }

    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        if (level < splitLevel) {
            handleDirective(splitDirector.characters(ch, start, length, level));
        }
    }

    @Override
    public void ignorableWhitespace(char[] ch, int start, int length) throws SAXException {
        if (level < splitLevel) {
            handleDirective(splitDirector.ignorableWhitespace(ch, start, length, level));
        }
    }

    @Override
    public void skippedEntity(String name) throws SAXException {
        if (level < splitLevel) {
            handleDirective(splitDirector.skippedEntity(name, level));
        }
    }

    public static enum SplitDirective { SPLIT, NO_SPLIT, SPLIT_NO_BYPASS, NO_SPLIT_BYPASS }
    
    public static class SplitDirector {

        private int recordCount = 0;
        
        public void reset() {
            recordCount = 0;
        }
        
        public SplitDirective startPrefixMapping(String prefix, String uri, int level) throws SAXException {
            return SplitDirective.NO_SPLIT;
        }

        public SplitDirective endPrefixMapping(String prefix, int level) throws SAXException {
            return SplitDirective.NO_SPLIT;
        }

        public SplitDirective startElement(String uri, String localName, String qName, Attributes atts, int level) throws SAXException {
            return SplitDirective.NO_SPLIT;
        }

        public SplitDirective endElement(String uri, String localName, String qName, int level) throws SAXException {
            return SplitDirective.NO_SPLIT;
        }

        public SplitDirective characters(char[] ch, int start, int length, int level) throws SAXException {
            System.out.println("called for characters " + new String(ch, start, length) + ", level=" + level);
            if (level == 2) {
                if (recordCount++ == 1) {
                    recordCount = 1; // the one we just entered
                    return SplitDirective.SPLIT;
                } else {
                    return SplitDirective.NO_SPLIT_BYPASS;
                }
            } else {
                return SplitDirective.NO_SPLIT;
            }
        }

        public SplitDirective ignorableWhitespace(char[] ch, int start, int length, int level) throws SAXException {
            return SplitDirective.NO_SPLIT;
        }

        public SplitDirective skippedEntity(String name, int level) throws SAXException {
            return SplitDirective.NO_SPLIT;
        }
        
    }
}
