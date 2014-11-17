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

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.Phaser;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
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
    private final ArrayDeque<StructuralStartEvent> startEventStack = new ArrayDeque<StructuralStartEvent>();

    public SplittingXMLFilter() {
        synchronousParser.setParent(this);
    }

    public static void main(String[] args) throws Exception {
        SimpleSplittingXMLFilter.main(args);
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
        splitState = SplitState.inter;
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

    protected final void splitStart() throws SAXException {
        splitState = SplitState.starting;
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
    
    protected void splitEnd() {
        splitState = SplitState.ending;
    }

    private enum SplitState { inter, starting, intra, ending }
    private SplitState splitState = SplitState.inter;

    private void writeSyntheticEndEvents() throws SAXException {
        Iterator<StructuralStartEvent> iter = startEventStack.iterator();
        while (iter.hasNext()) {
            StructuralStartEvent next = iter.next();
            System.out.println(next);
            switch (next.type) {
                case DOCUMENT:
                    super.endDocument();
                    break;
                case PREFIX_MAPPING:
                    super.endPrefixMapping(next.one);
                    break;
                case ELEMENT:
                    try {
                        super.endElement(next.one, next.two, next.three);
                    } catch (SAXException ex) {
                        System.out.println("culprit: "+next);
                        throw ex;
                    } catch (RuntimeException ex) {
                        System.out.println("culprit: "+next);
                        throw ex;
                    }
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

    @Override
    public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
        switch (splitState) {
            case inter:
                startEventStack.push(new StructuralStartEvent(uri, localName, qName, atts));
                break;
            case starting:
                splitState = SplitState.intra;
                break;
        }
        super.startElement(uri, localName, qName, atts);
    }

    @Override
    public void startPrefixMapping(String prefix, String uri) throws SAXException {
        switch (splitState) {
            case inter:
                startEventStack.push(new StructuralStartEvent(prefix, uri));
                break;
            case starting:
                splitState = SplitState.intra;
                break;
        }
        super.startPrefixMapping(prefix, uri);
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        super.endElement(uri, localName, qName);
        switch (splitState) {
            case inter:
                startEventStack.pop();
            case ending:
                splitState = SplitState.inter;
        }
    }

    @Override
    public void endPrefixMapping(String prefix) throws SAXException {
        switch (splitState) {
            case inter:
                startEventStack.pop();
            case ending:
                splitState = SplitState.inter;
        }
        super.endPrefixMapping(prefix);
    }
}
