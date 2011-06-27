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
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayDeque;
import java.util.Iterator;
import javax.xml.parsers.ParserConfigurationException;
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
import org.xml.sax.helpers.XMLFilterImpl;

/**
 *
 * @author Michael Gibney
 */
public class JoiningXMLFilter extends XMLFilterImpl {

    public static final String RESET_PROPERTY_NAME = "http://xml.org/sax/features/reset";
    private static final DevNullContentHandler devNullContentHandler = new DevNullContentHandler();
    private static final int RECORD_LEVEL = 1;

    private boolean initialized = false;
    private int level = -1;
    private final InitialEventContentHandler initialEventContentHandler = new InitialEventContentHandler();
    private final ArrayDeque<StructuralStartEvent> startEvents = new ArrayDeque<StructuralStartEvent>();
    private ContentHandler outputContentHandler;

    public static void main(String[] args) throws TransformerConfigurationException, SAXException, ParserConfigurationException, FileNotFoundException, IOException, TransformerException {
        TransformerFactory tf = TransformerFactory.newInstance("net.sf.saxon.TransformerFactoryImpl", null);
        Transformer t = tf.newTransformer();
        File inFile = new File("franklin-small-dump.xml");
        InputSource in = new InputSource(new BufferedInputStream(new FileInputStream(inFile)));
        JoiningXMLFilter joiner = new JoiningXMLFilterImpl();
        OutputStream out = new BufferedOutputStream(new FileOutputStream("split-joined.xml"));
        t.transform(new SAXSource(joiner, in), new StreamResult(out));
        out.close();
    }

    private static class JoiningXMLFilterImpl extends JoiningXMLFilter {

        public JoiningXMLFilterImpl() throws ParserConfigurationException, SAXException {
            SplittingXMLFilter splitter = new SplittingXMLFilter();
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
    public void reset() {
        initialized = false;
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

    @Override
    public void parse(InputSource input) throws SAXException, IOException {
        setupParse();
        super.parse(input);
    }

    @Override
    public void parse(String systemId) throws SAXException, IOException {
        setupParse();
        super.parse(systemId);
    }

    private void setupParse() {
        if (!initialized) {
            initialized = true;
            super.setContentHandler(initialEventContentHandler);
        } else {
            super.setContentHandler(devNullContentHandler);
        }
    }

    public void finished() throws SAXException {
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
            reset();
        } else {
            super.setFeature(name, value);
        }
    }

    @Override
    public boolean getFeature(String name) throws SAXNotRecognizedException, SAXNotSupportedException {
        if (RESET_PROPERTY_NAME.equals(name)) {
            try {
                return super.getFeature(name) && !initialized;
            } catch (SAXException ex) {
                return !initialized;
            }
        } else {
            return super.getFeature(name);
        }
    }

    @Override
    public void startDocument() throws SAXException {
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

}