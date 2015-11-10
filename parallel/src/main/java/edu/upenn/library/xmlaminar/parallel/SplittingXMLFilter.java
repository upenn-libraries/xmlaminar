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

package edu.upenn.library.xmlaminar.parallel;

import static edu.upenn.library.xmlaminar.parallel.JoiningXMLFilter.GROUP_BY_SYSTEMID_FEATURE_NAME;
import edu.upenn.library.xmlaminar.parallel.callback.OutputCallback;
import edu.upenn.library.xmlaminar.parallel.callback.StdoutCallback;
import edu.upenn.library.xmlaminar.parallel.callback.XMLReaderCallback;
import edu.upenn.library.xmlaminar.VolatileSAXSource;
import edu.upenn.library.xmlaminar.VolatileXMLFilterImpl;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import javax.xml.transform.sax.SAXSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXNotSupportedException;
import org.xml.sax.helpers.XMLFilterImpl;

/**
 *
 * @author Michael Gibney
 */
public class SplittingXMLFilter extends QueueSourceXMLFilter implements OutputCallback {

    private static final Logger LOG = LoggerFactory.getLogger(SplittingXMLFilter.class);
    private final AtomicBoolean parsing = new AtomicBoolean(false);
    private volatile Future<?> consumerTask;
    
    private int level = -1;
    private int startEventLevel = -1;
    private final ArrayDeque<StructuralStartEvent> startEventStack = new ArrayDeque<StructuralStartEvent>();

    public static void main(String[] args) throws Exception {
        LevelSplittingXMLFilter sxf = new LevelSplittingXMLFilter();
        sxf.setChunkSize(1);
        sxf.setOutputCallback(new StdoutCallback());
        sxf.setInputType(InputType.indirect);
        sxf.parse(new InputSource("../cli/whole-indirect.txt"));
    }
    
    public void ensureCleanInitialState() {
        if (level != -1) {
            throw new IllegalStateException("level != -1: "+level);
        }
        if (startEventLevel != -1) {
            throw new IllegalStateException("startEventLevel != -1: "+startEventLevel);
        }
        if (!startEventStack.isEmpty()) {
            throw new IllegalStateException("startEventStack not empty: "+startEventStack.size());
        }
        if (parsing.get()) {
            throw new IllegalStateException("already parsing");
        }
        if (consumerTask != null) {
            throw new IllegalStateException("consumerTask not null");
        }
        if (producerThrowable != null || consumerThrowable != null) {
            throw new IllegalStateException("throwable not null: "+producerThrowable+", "+consumerThrowable);
        }
        int arrived;
        if ((arrived = parseBeginPhaser.getArrivedParties()) > 0) {
            throw new IllegalStateException("parseBeginPhaser arrived count: "+arrived);
        }
        if ((arrived = parseEndPhaser.getArrivedParties()) > 0) {
            throw new IllegalStateException("parseEndPhaser arrived count: "+arrived);
        }
        if ((arrived = parseChunkDonePhaser.getArrivedParties()) > 0) {
            throw new IllegalStateException("parseChunkDonePhaser arrived count: "+arrived);
        }
    }
    
    @Override
    protected void initialParse(VolatileSAXSource in) {
        synchronousParser.setParent(this);
        setupParse(in.getInputSource());
    }

    @Override
    protected void repeatParse(VolatileSAXSource in) {
        reset(false);
        setupParse(in.getInputSource());
    }

    private void setupParse(InputSource in) {
        setContentHandler(synchronousParser);
        if (!parsing.compareAndSet(false, true)) {
            throw new IllegalStateException("failed setting parsing = true");
        }
        OutputLooper outputLoop = new OutputLooper(in, null, Thread.currentThread());
        consumerTask = getExecutor().submit(outputLoop);
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
        consumerTask = null;
        consumerThrowable = null;
        producerThrowable = null;
        if (splitDirector != null) {
            splitDirector.reset();
        }
        startEventStack.clear();
        if (!parsing.compareAndSet(false, false) && !cancel) {
            LOG.warn("at {}.reset(), parsing had been set to true", SplittingXMLFilter.class.getName(), new IllegalStateException());
        }
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
        } else if (GROUP_BY_SYSTEMID_FEATURE_NAME.equals(name)) {
            return false;
        }else {
            return super.getFeature(name);
        }
    }

    private final SynchronousParser synchronousParser = new SynchronousParser();

    @Override
    public boolean allowOutputCallback() {
        return true;
    }

    private class SynchronousParser extends VolatileXMLFilterImpl {

        @Override
        public void parse(InputSource input) throws SAXException, IOException {
            parse();
        }

        @Override
        public void parse(String systemId) throws SAXException, IOException {
            parse();
        }
        
        private void parse() throws SAXException, IOException {
            parseBeginPhaser.arrive();
            try {
                parseEndPhaser.awaitAdvanceInterruptibly(parseEndPhaser.arrive());
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            }
            parseChunkDonePhaser.arrive();
        }

    }
    
    private final Phaser parseBeginPhaser = new Phaser(2);
    private final Phaser parseEndPhaser = new Phaser(2);

    private final Phaser parseChunkDonePhaser = new Phaser(2);
    
    private class OutputLooper implements Runnable {

        private final InputSource input;
        private final String systemId;
        private final Thread producer;
        
        private OutputLooper(InputSource in, String systemId, Thread producer) {
            this.input = in;
            this.systemId = systemId;
            this.producer = producer;
        }
        
        private void parseLoop(InputSource input, String systemId) throws SAXException, IOException {
            if (input != null) {
                while (parsing.get()) {
                    outputCallback.callback(new VolatileSAXSource(synchronousParser, input));
                    try {
                        parseChunkDonePhaser.awaitAdvanceInterruptibly(parseChunkDonePhaser.arrive());
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            } else {
                throw new NullPointerException("null InputSource");
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
        } catch (Throwable t) {
            try {
                if (consumerThrowable == null) {
                    producerThrowable = t;
                    reset(true);
                } else {
                    t = consumerThrowable;
                    consumerThrowable = null;
                }
            } finally {
                if (outputCallback != null) {
                    outputCallback.finished(t);
                }
                throw new RuntimeException(t);
            }
        }
        outputCallback.finished(null);
    }

    @Override
    public XMLReaderCallback getOutputCallback() {
        return outputCallback;
    }

    @Override
    public void setOutputCallback(XMLReaderCallback xmlReaderCallback) {
        this.outputCallback = xmlReaderCallback;
    }

    private volatile XMLReaderCallback outputCallback;

    private volatile Throwable consumerThrowable;
    private volatile Throwable producerThrowable;

    @Override
    public void startDocument() throws SAXException {
        try {
            parseBeginPhaser.awaitAdvanceInterruptibly(parseBeginPhaser.arrive());
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
        startEventStack.push(new StructuralStartEvent());
        super.startDocument();
        level++;
        startEventLevel++;
    }

    @Override
    public void endDocument() throws SAXException {
        startEventLevel--;
        level--;
        super.endDocument();
        startEventStack.pop();
        if (!parsing.compareAndSet(true, false)) {
            throw new IllegalStateException("failed at endDocument setting parsing = false");
        }
        parseEndPhaser.arrive();
    }

    private int bypassLevel = Integer.MAX_VALUE;
    private int bypassStartEventLevel = Integer.MAX_VALUE;
    
    protected final void split() throws SAXException {
        writeSyntheticEndEvents();
        parseEndPhaser.arrive();
        try {
            parseBeginPhaser.awaitAdvanceInterruptibly(parseBeginPhaser.arrive());
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
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
        if (level < bypassLevel && handleDirective(splitDirector.startElement(uri, localName, qName, atts, level))) {
            startEventStack.push(new StructuralStartEvent(uri, localName, qName, atts));
        }
        super.startElement(uri, localName, qName, atts);
        level++;
        startEventLevel++;
    }

    @Override
    public void startPrefixMapping(String prefix, String uri) throws SAXException {
        if (level < bypassLevel && handleDirective(splitDirector.startPrefixMapping(prefix, uri, level))) {
            startEventStack.push(new StructuralStartEvent(prefix, uri));
        }
        super.startPrefixMapping(prefix, uri);
        startEventLevel++;
    }

    
    
    /**
     * Returns true if the generating event should be added to 
     * the startEvent stack
     * @param directive
     * @return
     * @throws SAXException 
     */
    private boolean handleDirective(SplitDirective directive) throws SAXException {
            switch (directive) {
                case SPLIT:
                    bypassLevel = level;
                    bypassStartEventLevel = startEventLevel;
                    split();
                    return false;
                case SPLIT_NO_BYPASS:
                    split();
                    return true;
                case NO_SPLIT_BYPASS:
                    bypassLevel = level;
                    bypassStartEventLevel = startEventLevel;
                    return false;
                case NO_SPLIT:
                    return true;
                default:
                    throw new AssertionError();
            }
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        startEventLevel--;
        level--;
        super.endElement(uri, localName, qName);
        if (handleStructuralEndEvent()) {
            handleDirective(splitDirector.endElement(uri, localName, qName, level));
        }
    }
    
    private boolean handleStructuralEndEvent() {
        if (level > bypassLevel) {
            return false;
        } else if (level < bypassLevel) {
            startEventStack.pop();
            return true;
        } else {
            if (startEventLevel == bypassStartEventLevel) {
                bypassEnd();
                return true;
            } else if (startEventLevel < bypassStartEventLevel) {
                startEventStack.pop();
                return true;
            } else {
                return false;
            }
        }
    }

    @Override
    public void endPrefixMapping(String prefix) throws SAXException {
        startEventLevel--;
        super.endPrefixMapping(prefix);
        if (handleStructuralEndEvent()) {
            handleDirective(splitDirector.endPrefixMapping(prefix, level));
        }
    }
    
    private void bypassEnd() {
        bypassLevel = Integer.MAX_VALUE;
        bypassStartEventLevel = Integer.MAX_VALUE;
    }

    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        if (level <= bypassLevel) {
            handleDirective(splitDirector.characters(ch, start, length, level));
        }
        super.characters(ch, start, length);
    }

    @Override
    public void ignorableWhitespace(char[] ch, int start, int length) throws SAXException {
        if (level <= bypassLevel) {
            handleDirective(splitDirector.ignorableWhitespace(ch, start, length, level));
        }
        super.ignorableWhitespace(ch, start, length);
    }

    @Override
    public void skippedEntity(String name) throws SAXException {
        if (level <= bypassLevel) {
            handleDirective(splitDirector.skippedEntity(name, level));
        }
        super.skippedEntity(name);
    }

    public static enum SplitDirective { SPLIT, NO_SPLIT, SPLIT_NO_BYPASS, NO_SPLIT_BYPASS }
    
    public static class SplitDirector {

        public void reset() {
            // default NOOP implementation
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
            return SplitDirective.NO_SPLIT;
        }

        public SplitDirective ignorableWhitespace(char[] ch, int start, int length, int level) throws SAXException {
            return SplitDirective.NO_SPLIT;
        }

        public SplitDirective skippedEntity(String name, int level) throws SAXException {
            return SplitDirective.NO_SPLIT;
        }
        
    }
    
}
