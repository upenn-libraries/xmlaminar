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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Scanner;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.regex.Pattern;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.stream.StreamResult;
import net.sf.saxon.event.ReceivingContentHandler;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLFilterImpl;

/**
 *
 * @author Michael Gibney
 */
public class JoiningXMLFilter extends XMLFilterImpl {

    public static final InputSource FINISHED = new InputSource();
    public static final String RESET_PROPERTY_NAME = "http://xml.org/sax/features/reset";
    protected static final ContentHandler devNullContentHandler = new DevNullContentHandler();
    private static final int RECORD_LEVEL = 1;

    private int level = -1;
    protected final ContentHandler initialEventContentHandler = new InitialEventContentHandler();
    private final ArrayDeque<StructuralStartEvent> startEvents = new ArrayDeque<StructuralStartEvent>();
    private ContentHandler outputContentHandler;
    private ExecutorService executor;

    private static final Pattern DEFAULT_DELIMITER_PATTERN = Pattern.compile(System.lineSeparator(), Pattern.LITERAL);
    private Pattern delimiterPattern = DEFAULT_DELIMITER_PATTERN;

    public static void main(String[] args) throws TransformerConfigurationException, SAXException, ParserConfigurationException, FileNotFoundException, IOException, TransformerException {
        TransformerFactory tf = TransformerFactory.newInstance("net.sf.saxon.TransformerFactoryImpl", null);
        Transformer t = tf.newTransformer();
        File inFile = new File("franklin-small-dump.xml");
        InputSource in = new InputSource(new BufferedInputStream(new FileInputStream(inFile)));
        JoiningXMLFilterImpl joiner = new JoiningXMLFilterImpl();
        OutputStream out = new BufferedOutputStream(new FileOutputStream("split-joined.xml"));
        try {
            t.transform(new SAXSource(joiner, in), new StreamResult(out));
        } finally {
            out.close();
            joiner.shutdown();
        }
    }

    private static class JoiningXMLFilterImpl extends JoiningXMLFilter {

        private final SplittingXMLFilter splitter;

        public void shutdown() {
            splitter.getExecutor().shutdown();
        }

        public JoiningXMLFilterImpl() throws ParserConfigurationException, SAXException {
            splitter = new SplittingXMLFilter();
            splitter.setExecutor(Executors.newCachedThreadPool());
            setParent(splitter);
            SAXParserFactory spf = SAXParserFactory.newInstance();
            spf.setNamespaceAware(true);
            splitter.setParent(spf.newSAXParser().getXMLReader());
            splitter.setXMLReaderCallback(new SplittingXMLFilter.XMLReaderCallback() {

                @Override
                public void callback(XMLReader reader, InputSource input) throws SAXException, IOException {
                    reader.setContentHandler(JoiningXMLFilterImpl.this);
                    reader.parse(input);
                }

                @Override
                public void callback(XMLReader reader, String systemId) throws SAXException, IOException {
                    reader.setContentHandler(JoiningXMLFilterImpl.this);
                    reader.parse(systemId);
                }
            });
        }

        @Override
        public void parse(InputSource input) throws SAXException, IOException {
            SplittingXMLFilter parent = (SplittingXMLFilter) getParent();
            parent.setContentHandler(this);
            parent.setDTDHandler(this);
            parent.setEntityResolver(this);
            parent.setErrorHandler(this);
            super.parse(input);
            finished();
        }

        @Override
        public void parse(String systemId) throws SAXException, IOException {
            throw new UnsupportedOperationException("Not supported yet.");
        }
    }

    private void reset() {
        level = -1;
        startEvents.clear();
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

    public void setDelimiterPattern(Pattern p) {
        delimiterPattern = p;
    }

    public Pattern getDelimiterPattern() {
        return delimiterPattern;
    }

    public ExecutorService getExecutor() {
        return executor;
    }

    public void setExecutor(ExecutorService executor) {
        this.executor = executor;
    }

    private Future<?> parseQueueSupplier;

    private void initParse() {
        if (parseQueue == null) {
            setParseQueue(new ArrayBlockingQueue<InputSource>(10, false));
        }
        if (executor == null) {
            executor = Executors.newFixedThreadPool(1, new ThreadFactory() {

                @Override
                public Thread newThread(Runnable r) {
                    Thread t = new Thread(r);
                    t.setDaemon(true);
                    return t;
                }
            });
        }
    }
    
    @Override
    public void parse(InputSource input) throws SAXException, IOException {
        reset();
        initParse();
        Runnable parseQueueRunner = new ParseQueueRunner(input);
        parseQueueSupplier = executor.submit(parseQueueRunner);
        InputSource next;
        try {
            if ((next = parseQueue.take()) != FINISHED) {
                setupParse(initialEventContentHandler);
                super.getParent().parse(next);
                while ((next = parseQueue.take()) != FINISHED) {
                    setupParse(devNullContentHandler);
                    super.getParent().parse(next);
                }
                finished();
            }
        } catch (InterruptedException ex) {
            throw new SAXException(ex);
        }
    }

    @Override
    public void parse(String systemId) throws SAXException, IOException {
        throw new UnsupportedOperationException("not yet implemented");
    }

    private BlockingQueue<InputSource> parseQueue;

    public void setParseQueue(BlockingQueue<InputSource> queue) {
        parseQueue = queue;
    }

    protected final boolean setupParse(ContentHandler handler) {
        super.setDTDHandler(this);
        super.setEntityResolver(this);
        super.setErrorHandler(this);
        super.setContentHandler(handler);
        return true;
    }

    public void finished() throws SAXException {
        super.setContentHandler(outputContentHandler);
        System.out.println("set outputCH, "+startEvents.size());
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
    public void startDocument() throws SAXException {
        System.out.println(super.getContentHandler());
        if (super.getContentHandler().getClass().equals(ReceivingContentHandler.class)) {
            Thread.dumpStack();
        }
        super.startDocument();
    }

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

    private static class DevNullContentHandler implements ContentHandler {

        @Override
        public void setDocumentLocator(Locator locator) {
        }

        @Override
        public void startDocument() throws SAXException {
        }

        @Override
        public void endDocument() throws SAXException {
        }

        @Override
        public void startPrefixMapping(String prefix, String uri) throws SAXException {
        }

        @Override
        public void endPrefixMapping(String prefix) throws SAXException {
        }

        @Override
        public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
        }

        @Override
        public void endElement(String uri, String localName, String qName) throws SAXException {
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

    private class ParseQueueRunner implements Runnable {

        private final InputSource input;

        public ParseQueueRunner(InputSource input) {
            this.input = input;
        }

        @Override
        public void run() {
            Reader r;
            if ((r = input.getCharacterStream()) == null) {
                InputStream in;
                if ((in = input.getByteStream()) != null) {
                    try {
                        r = new InputStreamReader(in, input.getEncoding());
                    } catch (UnsupportedEncodingException ex) {
                        throw new RuntimeException(ex);
                    }
                } else {
                    try {
                        r = new BufferedReader(new InputStreamReader(new URI(input.getSystemId()).toURL().openStream(), input.getEncoding()));
                    } catch (IOException ex) {
                        throw new RuntimeException(ex);
                    } catch (URISyntaxException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }
            try {
                Scanner s = new Scanner(r);
                s.useDelimiter(delimiterPattern);
                String next;
                while (s.hasNext()) {
                    next = s.next();
                    parseQueue.add(new InputSource(next));
                }
            } finally {
                parseQueue.add(FINISHED);
                try {
                    r.close();
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            }
        }
    }

}
