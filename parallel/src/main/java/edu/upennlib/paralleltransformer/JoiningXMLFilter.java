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

import edu.upennlib.paralleltransformer.QueueSourceXMLFilter.QueueSource;
import edu.upennlib.paralleltransformer.callback.OutputCallback;
import edu.upennlib.paralleltransformer.callback.StdoutCallback;
import edu.upennlib.paralleltransformer.callback.XMLReaderCallback;
import edu.upennlib.xmlutils.DevNullContentHandler;
import edu.upennlib.xmlutils.VolatileSAXSource;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.Phaser;
import java.util.concurrent.SynchronousQueue;
import java.util.logging.Level;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXNotSupportedException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLFilterImpl;

/**
 *
 * @author Michael Gibney
 */
public class JoiningXMLFilter extends QueueSourceXMLFilter implements OutputCallback, QueueSource<VolatileSAXSource> {

    private static final Logger LOG = LoggerFactory.getLogger(JoiningXMLFilter.class);
    protected static final ContentHandler devNullContentHandler = new DevNullContentHandler();
    private static final int RECORD_LEVEL = 1;

    private static final boolean DEFAULT_MULTI_OUT = true;
    private final boolean multiOut;
    private int level = -1;
    protected final ContentHandler initialEventContentHandler;
    private final ArrayDeque<StructuralStartEvent> startEvents;
    private ContentHandler outputContentHandler;

    public static void main(String[] args) throws Exception {
        //TXMLFilter txf = new TXMLFilter(new StreamSource("../cli/identity.xsl"), "/root/rec/@id", true, 1);
        JoiningXMLFilter preJoiner = new JoiningXMLFilter(true);
        preJoiner.setInputType(InputType.indirect);
        JoiningXMLFilter joiner = new JoiningXMLFilter(false);
        //txf.setInputType(InputType.indirect);
        joiner.setParent(preJoiner);
        File f1 = new File("/tmp/one.xml");
        File f2 = new File("/tmp/two.xml");
        Transformer t = TransformerFactory.newInstance().newTransformer();
        try {
            t.transform(new SAXSource(joiner, new InputSource("work/blah.txt")), new StreamResult(new SimulateClientDisconnect(new FileOutputStream(f1), 20)));
        } catch (Exception ex) {
            // nothing.
        }
        System.out.println("HERE");
        t.reset();
        t.transform(new SAXSource(joiner, new InputSource("work/blah.txt")), new StreamResult(new FileOutputStream(f2)));
        System.out.println("I THINK I'M DONE");
    }

    @Override
    public BlockingQueue<VolatileSAXSource> newQueue() {
        return new SynchronousQueue<VolatileSAXSource>();
    }
    
    private static class SimulateClientDisconnect extends FilterOutputStream {

        private final int limit;
        private int index;
        
        public SimulateClientDisconnect(OutputStream out, int limit) {
            super(out);
            this.limit = limit;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            out.write(b, off, len);
            if ((index += len) > limit) {
                close();
            }
        }

        @Override
        public void write(int b) throws IOException {
            out.write(b);
            if (++index > limit) {
                close();
            }
        }
        
    }
    
    public JoiningXMLFilter(boolean multiOut) {
        this.multiOut = multiOut;
        if (multiOut) {
            parseBeginPhaser = new Phaser(2);
            parseEndPhaser = new Phaser(2);
            this.synchronousParser = new SynchronousParser(this);
        } else {
            parseBeginPhaser = null;
            parseEndPhaser = null;
            this.synchronousParser = null;
        }
        initialEventContentHandler = new InitialEventContentHandler();
        startEvents = new ArrayDeque<StructuralStartEvent>();
    }
    
    public JoiningXMLFilter() {
        this(DEFAULT_MULTI_OUT);
    }
    
    private void reset() {
        level = -1;
        startEvents.clear();
    }
    
    private void hardReset() {
        producerThrowable = null;
        consumerThrowable = null;
        if (multiOut) {
            consumerTask = null;
            lastSystemId = null;
            callbackQueue.clear();
            resetPhaser(parseBeginPhaser);
            resetPhaser(parseEndPhaser);
        }
    }

    private static void resetPhaser(Phaser phaser) {
        while (phaser.getArrivedParties() > 0) {
            phaser.arrive();
        }
    }

    @Override
    public ContentHandler getContentHandler() {
        return outputContentHandler;
    }

    @Override
    public void setContentHandler(ContentHandler handler) {
        outputContentHandler = handler;
        super.setContentHandler(handler);
    }

    @Override
    public void endDocument() throws SAXException {
        super.endDocument();
    }

    @Override
    public void endPrefixMapping(String prefix) throws SAXException {
        super.endPrefixMapping(prefix);
    }

    @Override
    public void parse(InputSource input) throws SAXException, IOException {
        if (multiOut) {
            parseMultiOut(input, null);
        } else {
            parse(input, null);
        }
    }
    
    @Override
    public void parse(String systemId) throws SAXException, IOException {
        if (multiOut) {
            parseMultiOut(null, systemId);
        } else {
            parse(null, systemId);
        }
    }
    
    private void parseMultiOut(InputSource input, String systemId) throws SAXException, IOException {
        reset();
        hardReset();
//        try {
            if (input != null) {
                super.parse(input);
            } else {
                super.parse(systemId);
            }
//        } catch (Throwable t) {
//            try {
//                if (consumerThrowable == null) {
//                    producerThrowable = t;
//                    consumerTask.cancel(true);
//                } else {
//                    t = consumerThrowable;
//                    consumerThrowable = null;
//                }
//            } finally {
//                outputCallback.finished(t);
//                throw new RuntimeException(t);
//            }
//        }
        outputCallback.finished(null);
    }

    private void parse(InputSource input, String systemId) throws SAXException, IOException {
        reset();
        hardReset();
        if (input != null) {
            super.parse(input);
        } else {
            super.parse(systemId);
        }
    }

    private String lastSystemId;

    @Override
    public void initialParse(VolatileSAXSource in) {
        if (multiOut) {
            setContentHandler(synchronousParser);
        }
        setupParse(initialEventContentHandler);
        if (multiOut) {
//            OutputLooper outLoop = new OutputLooper(Thread.currentThread());
//            consumerTask = getExecutor().submit(outLoop);
            lastSystemId = in.getSystemId();
            if (true || getInputType() != InputType.queue) {
                getExecutor().submit(new AsyncOutput(outputCallback, synchronousParser, in.getInputSource()));
            } else {
                try {
                    outputCallback.callback(new VolatileSAXSource(synchronousParser, in.getInputSource()));
                    //callbackQueue.put(in.getInputSource());
                } catch (SAXException ex) {
                    throw new RuntimeException(ex);
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            }
        }
    }
    
    private static class AsyncOutput implements Runnable {

        private final XMLReaderCallback outputCallback;
        private final XMLReader xmlReader;
        private final InputSource in;

        public AsyncOutput(XMLReaderCallback outputCallback, XMLReader xmlReader, InputSource in) {
            this.outputCallback = outputCallback;
            this.xmlReader = xmlReader;
            this.in = in;
        }
        
        @Override
        public void run() {
            try {
                outputCallback.callback(new VolatileSAXSource(xmlReader, in));
                //callbackQueue.put(in.getInputSource());
            } catch (SAXException ex) {
                throw new RuntimeException(ex);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
        
    }
    
    @Override
    public void repeatParse(VolatileSAXSource in) {
        if (!multiOut) {
            setupParse(devNullContentHandler);
        } else {
            String systemId = in.getSystemId();
            System.err.println("systemId compare: "+systemId+", "+lastSystemId);
            if (systemId == null ? lastSystemId == null : systemId.equals(lastSystemId)) {
                setupParse(devNullContentHandler);
            } else {
                lastSystemId = systemId;
                try {
                    localFinished();
                } catch (SAXException ex) {
                    throw new RuntimeException(ex);
                }
                reset();
                setupParse(initialEventContentHandler);
                if (true || getInputType() != InputType.queue) {
                    getExecutor().submit(new AsyncOutput(outputCallback, synchronousParser, in.getInputSource()));
                } else {
                    try {
                        outputCallback.callback(new VolatileSAXSource(synchronousParser, in.getInputSource()));
                        //callbackQueue.put(in.getInputSource());
                    } catch (SAXException ex) {
                        throw new RuntimeException(ex);
                    } catch (IOException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }
        }
    }

    private void localFinished() throws SAXException {
        super.setContentHandler(outputContentHandler);
        Iterator<StructuralStartEvent> iter = startEvents.iterator();
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
    
    @Override
    public void finished() throws SAXException {
        localFinished();
        if (multiOut) {
            //outputCallback.finished(null);
        }
    }

    private static final InputSource END_OUTPUT_LOOP = new InputSource();
    
    private final BlockingQueue<InputSource> callbackQueue = new SynchronousQueue<InputSource>();

    @Override
    public boolean allowOutputCallback() {
        return multiOut;
    }

    @Override
    public boolean getFeature(String name) throws SAXNotRecognizedException, SAXNotSupportedException {
        if (GROUP_BY_SYSTEMID_FEATURE_NAME.equals(name)) {
            return true;
        } else {
            return super.getFeature(name);
        }
    }
    
    public static final String GROUP_BY_SYSTEMID_FEATURE_NAME = "http://transform.xml.javax/Transformer#groupBySystemId";
    
    private class OutputLooper implements Runnable {

        private final Thread producer;
        
        private OutputLooper(Thread producer) {
            this.producer = producer;
        }
        
        private void relayOutputCallbacks() throws SAXException, IOException, InterruptedException {
            InputSource in;
            while ((in = callbackQueue.take()) != END_OUTPUT_LOOP) {
                outputCallback.callback(new VolatileSAXSource(synchronousParser, in));
            }
        }

        @Override
        public void run() {
            try {
                relayOutputCallbacks();
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
    
    private Throwable producerThrowable;
    private Throwable consumerThrowable;
    private Future<?> consumerTask;

    @Override
    public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
        super.startElement(uri, localName, qName, atts);
        if (++level == RECORD_LEVEL - 1) {
            super.setContentHandler(outputContentHandler);
        }
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        if (level-- == RECORD_LEVEL - 1) {
            super.setContentHandler(devNullContentHandler);
        }
        super.endElement(uri, localName, qName);
    }

    private XMLReaderCallback outputCallback;
    
    @Override
    public XMLReaderCallback getOutputCallback() {
        return outputCallback;
    }

    @Override
    public void setOutputCallback(XMLReaderCallback callback) {
        if (!multiOut) {
            throw new IllegalStateException("outputCallback only relevant for joiner if multiOut==true");
        }
        this.outputCallback = callback;
    }
    
    private final Phaser parseBeginPhaser;
    private final Phaser parseEndPhaser;
    private final XMLFilterImpl synchronousParser;
    
    private class SynchronousParser extends XMLFilterImpl {

        private SynchronousParser(XMLReader parent) {
            super(parent);
        }
        
        @Override
        public void parse(String systemId) throws SAXException, IOException {
            parse();
        }

        @Override
        public void parse(InputSource input) throws SAXException, IOException {
            parse();
        }

        @Override
        public void startDocument() throws SAXException {
            try {
                parseBeginPhaser.awaitAdvanceInterruptibly(parseBeginPhaser.arrive());
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            }
            super.startDocument();
        }
        
        @Override
        public void endDocument() throws SAXException {
            super.endDocument();
            parseEndPhaser.arrive();
        }

        private void parse() throws SAXException, IOException {
            parseBeginPhaser.arrive();
            try {
                parseEndPhaser.awaitAdvanceInterruptibly(parseEndPhaser.arrive());
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            }
        }
        
    }
    
    private class InitialEventContentHandler implements ContentHandler {

        @Override
        public void setDocumentLocator(Locator locator) {
        }

        @Override
        public void startDocument() throws SAXException {
            startEvents.push(new StructuralStartEvent());
            outputContentHandler.startDocument();
        }

        @Override
        public void endDocument() throws SAXException {
            throw new UnsupportedOperationException("Not supported.");
        }

        @Override
        public void startPrefixMapping(String prefix, String uri) throws SAXException {
            startEvents.push(new StructuralStartEvent(prefix, uri));
            outputContentHandler.startPrefixMapping(prefix, uri);
        }

        @Override
        public void endPrefixMapping(String prefix) throws SAXException {
            throw new UnsupportedOperationException("Not supported.");
        }

        @Override
        public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
            startEvents.push(new StructuralStartEvent(uri, localName, qName, atts));
            outputContentHandler.startElement(uri, localName, qName, atts);
        }

        @Override
        public void endElement(String uri, String localName, String qName) throws SAXException {
            throw new UnsupportedOperationException("Not supported.");
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

    }

}
