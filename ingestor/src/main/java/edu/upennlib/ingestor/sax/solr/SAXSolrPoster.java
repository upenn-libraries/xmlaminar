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

package edu.upennlib.ingestor.sax.solr;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.util.Collection;
import java.util.LinkedHashSet;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.sax.SAXTransformerFactory;
import javax.xml.transform.sax.TransformerHandler;
import javax.xml.transform.stream.StreamResult;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.StreamingUpdateSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;

/**
 *
 * @author michael
 */
public class SAXSolrPoster implements ContentHandler {

    public static final String TRANSFORMER_FACTORY_CLASS_NAME = "net.sf.saxon.TransformerFactoryImpl";
    public static final int DOC_CHUNK_SIZE = 20;
    private int level = -1;
    private boolean inDoc = false;
    private boolean inField = false;
    private String fieldName = null;
    private final ByteArrayOutputStream fieldContentsOut = new ByteArrayOutputStream();
    private final OutputStreamWriter fieldContentsWriter;
    private SolrInputDocument workingInputDocument = null;
    private Collection<SolrInputDocument> inputDocsChunk = new LinkedHashSet<SolrInputDocument>();
    private final TransformerHandler transformerInput;
    private final Transformer t;
    private boolean useTransformer = false;
    private int transformerRefLevel = -1;
    private boolean closeTransformer = false;
    private Logger logger = Logger.getLogger(getClass());
    private StreamingUpdateSolrServer server;
    public final boolean DELETE = true;
    private boolean delete = DELETE;
    private String solrUrl;
    private int queueSize;
    private int threadCount;

    public String getSolrUrl() {
        return solrUrl;
    }

    public void setSolrUrl(String solrUrl) {
        this.solrUrl = solrUrl;
    }

    public int getQueueSize() {
        return queueSize;
    }

    public void setQueueSize(int queueSize) {
        this.queueSize = queueSize;
    }

    public int getThreadCount() {
        return threadCount;
    }

    public void setThreadCount(int threadCount) {
        this.threadCount = threadCount;
    }

    public void initSpring() {
        try {
            setServer(new StreamingUpdateSolrServer(solrUrl, queueSize, threadCount));
        } catch (MalformedURLException ex) {
            throw new RuntimeException(ex);
        }
    }

    public void setServer(StreamingUpdateSolrServer server) {
        this.server = server;
    }

    public StreamingUpdateSolrServer getServer() {
        return server;
    }

    public void setDelete(boolean delete) {
        this.delete = delete;
    }

    public boolean isDelete() {
        return delete;
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
        logger.trace("ignoring setDocumentLocator("+locator+")");
    }

    @Override
    public void startDocument() throws SAXException {
        if (delete) {
            try {
                server.deleteByQuery("*:*");
            } catch (SolrServerException ex) {
                throw new RuntimeException(ex);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    @Override
    public void endDocument() throws SAXException {
        if (docsCount > 0) {
            submitDocs();
        }
        try {
            server.commit();
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

}
