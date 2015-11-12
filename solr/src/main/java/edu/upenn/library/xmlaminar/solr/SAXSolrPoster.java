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

package edu.upenn.library.xmlaminar.solr;

import edu.upenn.library.xmlaminar.VolatileSAXSource;
import edu.upenn.library.xmlaminar.parallel.DummyXMLReader;
import edu.upenn.library.xmlaminar.parallel.QueueSourceXMLFilter;
import java.io.BufferedReader;
import org.apache.solr.client.solrj.SolrServer;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.zip.GZIPInputStream;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.sax.SAXResult;
import javax.xml.transform.sax.SAXTransformerFactory;
import javax.xml.transform.sax.TransformerHandler;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrServer;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.Attributes;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;


/**
 *
 * @author michael
 */
public class SAXSolrPoster extends QueueSourceXMLFilter {

    public static final String TRANSFORMER_FACTORY_CLASS_NAME = "net.sf.saxon.TransformerFactoryImpl";
    private SolrServer server;

    private int level = -1;
    private boolean inDoc = false;
    private boolean inField = false;
    private String fieldName = null;
    private final ByteArrayOutputStream fieldContentsOut = new ByteArrayOutputStream();
    private final OutputStreamWriter fieldContentsWriter;
    private SolrInputDocument workingInputDocument = null;
    private final Collection<SolrInputDocument> inputDocsChunk = new LinkedHashSet<>();
    private final TransformerHandler transformerInput;
    private final Transformer t;
    private boolean useTransformer = false;
    private int transformerRefLevel = -1;
    private boolean closeTransformer = false;
    private static final Logger logger = LoggerFactory.getLogger(SAXSolrPoster.class);

    public void reset() {
        level = -1;
        inDoc = false;
        inField = false;
        fieldName = null;
        workingInputDocument = null;
        useTransformer = false;
        transformerRefLevel = -1;
        closeTransformer = false;
        try {
            submitDocs();
        } catch (SolrRuntimeException ex) {
            docsCount = 0;
            inputDocsChunk.clear();
            // Ignore this.  If resetting is being manually called, it's likely to happen anyway.
        }
        try {
            fieldContentsWriter.flush();
        } catch (IOException ex) {
            throw new RuntimeException("this should never happen", ex);
        }
    }

    @Override
    public void shutdown() {
        if (server instanceof ConcurrentUpdateSolrServer) {
            ((ConcurrentUpdateSolrServer) server).blockUntilFinished();
        }
        server.shutdown();
    }

    public void setServer(SolrServer server) {
        this.server = server;
    }
    
    public static void main(String[] args) throws IOException, ParserConfigurationException, SAXException, TransformerConfigurationException, TransformerException {
        ConcurrentUpdateSolrServerFactory ssf = new ConcurrentUpdateSolrServerFactory();
        ssf.setThreadCount(4);
        ssf.setQueueSize(10);
        ssf.setSolrURL(args[0]);
        SAXSolrPoster ssp = new SAXSolrPoster();
        ssp.setServer(ssf.getServer());
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        TransformerFactory tf = TransformerFactory.newInstance("net.sf.saxon.TransformerFactoryImpl", null);
        try {
            String line;
            while ((line = in.readLine()) != null) {
                if (line.length() > 0) {
                    Transformer t = tf.newTransformer();
                    StreamSource s;
                    if (!line.endsWith(".gz")) {
                        s = new StreamSource(line);
                    } else {
                        s = new StreamSource(new GZIPInputStream(new FileInputStream(line)));
                        s.setSystemId(line);
                    }
                    t.transform(s, new SAXResult(ssp));
                }
            }
        } finally {
            in.close();
        }
        ssp.shutdown();
    }

    public SolrServer getServer() {
        return server;
    }

    public SAXSolrPoster() {
        SAXTransformerFactory tf = (SAXTransformerFactory)TransformerFactory.newInstance(TRANSFORMER_FACTORY_CLASS_NAME, null);
        try {
            transformerInput = tf.newTransformerHandler();
        } catch (TransformerConfigurationException ex) {
            throw new RuntimeException(ex);
        }
        transformerInput.setResult(new StreamResult(fieldContentsOut));
        t = transformerInput.getTransformer();
        t.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
        t.setOutputProperty(OutputKeys.ENCODING, "UTF8");
        t.setOutputProperty(OutputKeys.INDENT, "yes");
        try {
            fieldContentsWriter = new OutputStreamWriter(fieldContentsOut, "UTF8");
        } catch (UnsupportedEncodingException ex) {
            throw new RuntimeException(ex);
        }
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
        docsCount++;
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

    private void submitDocs() throws SolrRuntimeException {
        try {
            server.add(inputDocsChunk);
        } catch (SolrServerException ex) {
            throw new SolrRuntimeException(ex);
        } catch (IOException ex) {
            throw new SolrRuntimeException(ex);
        } catch (SolrException ex) {
            throw new SolrRuntimeException(ex);
        }
        docsCount = 0;
        inputDocsChunk.clear();
    }

    private void openTransformer(int refLevel) throws SAXException {
        try {
            fieldContentsWriter.flush();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        useTransformer = true;
        transformerRefLevel = refLevel;
        transformerInput.startDocument();
    }

    private void closeTransformer() throws SAXException {
        closeTransformer = false;
        useTransformer = false;
        transformerInput.endDocument();
    }

    @Override
    public void setDocumentLocator(Locator locator) {
        if (logger.isTraceEnabled()) {
            logger.trace("ignoring setDocumentLocator("+locator+")");
        }
    }

    @Override
    public void startDocument() throws SAXException {
        // Nothing needs to be done here.
    }

    @Override
    public void endDocument() throws SAXException, SolrRuntimeException {
        if (docsCount > 0) {
            submitDocs();
        }
    }

    @Override
    protected void initialParse(VolatileSAXSource in) {
        synchronousParse(in);
    }

    @Override
    protected void repeatParse(VolatileSAXSource in) {
        synchronousParse(in);
    }

    private static final DummyXMLReader dxr = new DummyXMLReader();

    private void synchronousParse(VolatileSAXSource in) {
        XMLReader xmlReader = in.getXMLReader();
        xmlReader.setContentHandler(this);
        try {
            xmlReader.parse(in.getInputSource());
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        } catch (SAXException ex) {
            throw new RuntimeException(ex);
        }
        in.setXMLReader(dxr);
    }

    @Override
    protected void finished() throws SAXException {
        shutdown();
    }

    public static class SolrRuntimeException extends RuntimeException {

        public SolrRuntimeException(Throwable cause) {
            super(cause);
        }
        
    }

    @Override
    public void startPrefixMapping(String prefix, String uri) throws SAXException {
        if (inField) {
            if (!useTransformer) {
                openTransformer(level + 1);
            } else if (closeTransformer) {
                closeTransformer();
                openTransformer(level + 1);
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
                openTransformer(level);
            } else if (closeTransformer) {
                closeTransformer();
                openTransformer(level);
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

}
