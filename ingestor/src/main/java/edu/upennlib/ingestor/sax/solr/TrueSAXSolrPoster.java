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

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package edu.upennlib.ingestor.sax.solr;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.util.Collection;
import java.util.HashSet;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.stream.StreamResult;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.TTCCLayout;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.StreamingUpdateSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.DTDHandler;
import org.xml.sax.EntityResolver;
import org.xml.sax.ErrorHandler;
import org.xml.sax.InputSource;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXNotSupportedException;
import org.xml.sax.XMLReader;
import org.xml.sax.ext.LexicalHandler;
import org.xml.sax.helpers.XMLFilterImpl;

/**
 *
 * @author michael
 */
public class TrueSAXSolrPoster implements ContentHandler, XMLReader {

    public static final String TRANSFORMER_FACTORY_CLASS_NAME = "net.sf.saxon.TransformerFactoryImpl";
    public static final int DOC_CHUNK_SIZE = 20;
    private int level = -1;
    private boolean inDoc = false;
    private boolean inField = false;
    private String fieldName = null;
    private final ByteArrayOutputStream fieldContentsOut = new ByteArrayOutputStream();
    private final OutputStreamWriter fieldContentsWriter;
    private SolrInputDocument workingInputDocument = null;
    private Collection<SolrInputDocument> inputDocsChunk = new HashSet<SolrInputDocument>();
    private final XMLFilterImpl transformerInput = new XMLFilterImpl();
    private final SAXSource transformerInputSource = new SAXSource(transformerInput, new InputSource());
    private final Transformer t;
    private boolean useTransformer = false;
    private int transformerRefLevel = -1;
    private boolean closeTransformer = false;
    private final TransformerRunner transformerRunner = new TransformerRunner();
    private Thread transformerRunnerThread;
    private Logger logger = Logger.getLogger(getClass());
    private StreamingUpdateSolrServer server;
    public final boolean DELETE = true;

    public void setServer(StreamingUpdateSolrServer server) {
        this.server = server;
    }

    public TrueSAXSolrPoster() {
        TransformerFactory tf = TransformerFactory.newInstance(TRANSFORMER_FACTORY_CLASS_NAME, null);
        try {
            t = tf.newTransformer();
        } catch (TransformerConfigurationException ex) {
            throw new RuntimeException(ex);
        }
        t.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
        t.setOutputProperty(OutputKeys.ENCODING, "UTF8");
        t.setOutputProperty(OutputKeys.INDENT, "yes");
        try {
            fieldContentsWriter = new OutputStreamWriter(fieldContentsOut, "UTF8");
        } catch (UnsupportedEncodingException ex) {
            throw new RuntimeException(ex);
        }
        logger.addAppender(new ConsoleAppender(new TTCCLayout(), "System.err"));
        logger.setLevel(Level.WARN);
    }

    private void enterDoc() {
        workingInputDocument = new SolrInputDocument();
        inDoc = true;
    }

    private int docsCount = 0;

    private void exitDoc() {
        inputDocsChunk.add(workingInputDocument);
        workingInputDocument = null;
        inDoc = false;
        if (docsCount++ >= DOC_CHUNK_SIZE) {
            submitDocs();
        }
    }

    private void enterField(String fieldName) {
        inField = true;
        useTransformer = false;
        this.fieldName = fieldName;
        fieldContentsOut.reset();
    }

    private void exitField() throws SAXException {
        if (closeTransformer) {
            closeTransformer();
        } else {
            try {
                fieldContentsWriter.flush();
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
        try {
            workingInputDocument.addField(fieldName, fieldContentsOut.toString("UTF8"));
        } catch (UnsupportedEncodingException ex) {
            throw new RuntimeException(ex);
        }
        inField = false;
        fieldName = null;
    }

    private void submitDocs() {
        try {
            server.add(inputDocsChunk);
        } catch (SolrServerException ex) {
            throw new RuntimeException(ex);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        docsCount = 0;
        inputDocsChunk.clear();
    }

    private void openTransformer(int refLevel, String extra) throws SAXException {
        transformerRunner.blockUntilFinished();
        try {
            fieldContentsWriter.flush();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        useTransformer = true;
        transformerRefLevel = refLevel;
        if (transformerRunnerThread == null) {
            transformerRunnerThread = new Thread(transformerRunner, "transformerRunnerThread");
            transformerRunnerThread.start();
        } else {
            transformerRunner.openedTransformer();
        }
        synchronized (transformerParsing) {
            while (!transformerParsing[0]) {
                try {
                    transformerParsing.wait();
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
            }
        }
        transformerInput.startDocument();
    }

    private void closeTransformer() throws SAXException {
        closeTransformer = false;
        useTransformer = false;
        transformerInput.endDocument();
        synchronized(transformerParsing) {
            transformerParsing[0] = false;
            transformerParsing.notify();
        }
    }

    @Override
    public void setDocumentLocator(Locator locator) {
        logger.trace("ignoring setDocumentLocator("+locator+")");
    }

    @Override
    public void startDocument() throws SAXException {
        if (DELETE) {
            try {
                server.deleteByQuery("*:*");
            } catch (SolrServerException ex) {
                throw new RuntimeException(ex);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
        transformerInput.setParent(this);
    }

    @Override
    public void endDocument() throws SAXException {
        if (docsCount > 0) {
            submitDocs();
        }
        try {
            server.commit();
            transformerRunner.finished();
        } catch (SolrServerException ex) {
            throw new RuntimeException(ex);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void startPrefixMapping(String prefix, String uri) throws SAXException {
        if (inField) {
            if (!useTransformer) {
                openTransformer(level + 1, "startPrefixMapping("+prefix+", "+uri+")");
            } else if (closeTransformer) {
                closeTransformer();
                openTransformer(level + 1, "startPrefixMapping("+prefix+", "+uri+")");
            }
            transformerInput.startPrefixMapping(prefix, uri);
        }
    }

    @Override
    public void endPrefixMapping(String prefix) throws SAXException {
        if (inField) {
            if (!useTransformer) {
                throw new RuntimeException();
            }
            transformerInput.endPrefixMapping(prefix);
        }
    }

    @Override
    public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
        level++;
        if (!inDoc && level == 1) {
            enterDoc();
        } else if (!inField && level == 2) {
            enterField(atts.getValue("name"));
        } else if (inField) {
            if (!useTransformer) {
                openTransformer(level, "startElement("+uri+", "+localName+", "+qName+")");
            } else if (closeTransformer) {
                closeTransformer();
                openTransformer(level, "startElement("+uri+", "+localName+", "+qName+")");
            }
            transformerInput.startElement(uri, localName, qName, atts);
        }
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        if (inField && level == 2) {
            exitField();
        } else if (inDoc && level == 1) {
            exitDoc();
        } else if (inField) {
            if (!useTransformer) {
                throw new RuntimeException();
            } else if (transformerRefLevel == level) {
                closeTransformer = true;
            }
            transformerInput.endElement(uri, localName, qName);
        }
        level--;
    }

    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        if (useTransformer && !closeTransformer) {
            transformerInput.characters(ch, start, length);
        } else if (inField) {
            if (closeTransformer) {
                closeTransformer();
            }
            try {
                fieldContentsWriter.write(ch, start, length);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    @Override
    public void ignorableWhitespace(char[] ch, int start, int length) throws SAXException {
        if (useTransformer && !closeTransformer) {
            transformerInput.ignorableWhitespace(ch, start, length);
        } else if (inField) {
            if (closeTransformer) {
                closeTransformer();
            }
            try {
                fieldContentsWriter.write(ch, start, length);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    @Override
    public void processingInstruction(String target, String data) throws SAXException {
        logger.warn("ignoring processingInstruction("+target+", "+data+")");
    }

    @Override
    public void skippedEntity(String name) throws SAXException {
        logger.warn("ignoring skippedEntity("+name+")");
    }

    @Override
    public boolean getFeature(String name) throws SAXNotRecognizedException, SAXNotSupportedException {
        if ("http://xml.org/sax/features/namespaces".equals(name)) {
            return true;
        } else {
            throw new UnsupportedOperationException("getFeature("+name+")");
        }
    }

    @Override
    public void setFeature(String name, boolean value) throws SAXNotRecognizedException, SAXNotSupportedException {
        if ("http://xml.org/sax/features/namespaces".equals(name)) {
            if (!value) {
                throw new UnsupportedOperationException("cannot set namespaces feature to false");
            }
        } else if ("http://xml.org/sax/features/namespace-prefixes".equals(name)) {
            logger.trace("ignoring setFeature("+name+", "+value+")");
        } else if ("http://xml.org/sax/features/validation".equals(name)) {
            logger.trace("ignoring setFeature("+name+", "+value+")");
        } else {
            throw new UnsupportedOperationException("setFeature("+name+", "+value+")");
        }
    }

    @Override
    public Object getProperty(String name) throws SAXNotRecognizedException, SAXNotSupportedException {
        if ("http://xml.org/sax/properties/lexical-handler".equals(name)) {
            return lexicalHandler;
        } else {
            throw new UnsupportedOperationException("getProperty("+name+")");
        }
    }
    LexicalHandler lexicalHandler = null;
    @Override
    public void setProperty(String name, Object value) throws SAXNotRecognizedException, SAXNotSupportedException {
        if ("http://xml.org/sax/properties/lexical-handler".equals(name)) {
            lexicalHandler = (LexicalHandler)value;
        } else {
            throw new UnsupportedOperationException("setFeature("+name+", "+value+")");
        }
    }

    @Override
    public void setEntityResolver(EntityResolver resolver) {
        logger.trace("ignoring setEntityResolver("+resolver+")");
    }

    @Override
    public EntityResolver getEntityResolver() {
        throw new UnsupportedOperationException("getEntityResolver() not supported");
    }

    @Override
    public void setDTDHandler(DTDHandler handler) {
        dtdHandler = handler;
    }
    DTDHandler dtdHandler = null;
    @Override
    public DTDHandler getDTDHandler() {
        return dtdHandler;
    }

    @Override
    public void setContentHandler(ContentHandler handler) {
        logger.trace("ignoring setContentHandler("+handler+")");
    }

    @Override
    public ContentHandler getContentHandler() {
        return null;
    }

    @Override
    public void setErrorHandler(ErrorHandler handler) {
        errorHandler = handler;
    }
    private ErrorHandler errorHandler = null;
    @Override
    public ErrorHandler getErrorHandler() {
        return errorHandler;
    }

    private final boolean[] transformerParsing = new boolean[1];

    @Override
    public void parse(InputSource input) throws IOException, SAXException {
        if (transformerParsing[0]) {
            throw new IllegalStateException();
        }
        synchronized (transformerParsing) {
            transformerParsing[0] = true;
            transformerParsing.notify();
            while (transformerParsing[0]) {
                try {
                    transformerParsing.wait();
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
            }
        }
    }

    @Override
    public void parse(String systemId) throws IOException, SAXException {
        if (transformerParsing[0]) {
            throw new IllegalStateException();
        }
        synchronized (transformerParsing) {
            transformerParsing[0] = true;
            transformerParsing.notify();
            while (transformerParsing[0]) {
                try {
                    transformerParsing.wait();
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
            }
        }
    }


    private class TransformerRunner implements Runnable {

        private boolean finished = false;
        private boolean transformationRunning = false;

        public void blockUntilFinished() {
            synchronized (this) {
                while (transformationRunning) {
                    try {
                        wait();
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }
        }

        public void finished() {
            finished = true;
            synchronized (this) {
                notify();
            }
        }

        public void openedTransformer() {
            synchronized (this) {
                transformationRunning = true;
                notify();
            }
        }

        @Override
        public void run() {
            synchronized (this) {
                while (!finished) {
                    if (!useTransformer) {
                        try {
                            transformationRunning = false;
                            notify();
                            wait();
                        } catch (InterruptedException ex) {
                            throw new RuntimeException(ex);
                        }
                    } else {
                        try {
                            t.transform(transformerInputSource, new StreamResult(fieldContentsOut));
                        } catch (TransformerException ex) {
                            throw new RuntimeException(ex);
                        }
                    }
                }
            }
        }

    }

}
