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

package edu.upennlib.ingestor.sax.integrator;

import edu.upennlib.ingestor.sax.xsl.BufferingXMLFilter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.Reader;
import java.io.StringReader;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import javax.xml.transform.TransformerException;
import net.sf.saxon.lib.StandardErrorListener;
import org.xml.sax.ContentHandler;
import org.xml.sax.DTDHandler;
import org.xml.sax.EntityResolver;
import org.xml.sax.ErrorHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXNotSupportedException;
import org.xml.sax.XMLReader;
import org.apache.log4j.Logger;
import org.xml.sax.ext.LexicalHandler;
import org.xml.sax.helpers.AttributesImpl;

/**
 *
 * @author michael
 */
public abstract class SQLXMLReader implements XMLReader {

    public static final String TRANSFORMER_FACTORY_CLASS_NAME = "net.sf.saxon.TransformerFactoryImpl";
    Logger logger = Logger.getLogger(getClass());
    Connection connection;
    ResultSet rs;
    ContentHandler ch;
    ErrorHandler eh;
    DTDHandler dh;
    LexicalHandler lh;
    Class expectedInputImplementation;

    public SQLXMLReader(Class expectedInputImplementation) {
        this.expectedInputImplementation = expectedInputImplementation;
    }

    public void setIdFieldLabels(String[] idFieldLabels) {
        this.idFieldLabels = idFieldLabels;
    }

    public void setOutputFieldLabels(String[] outputFieldLabels) {
        this.outputFieldLabels = outputFieldLabels;
    }

    @Override
    public boolean getFeature(String name) throws SAXNotRecognizedException, SAXNotSupportedException {
        if (name.equals("http://xml.org/sax/features/namespaces")) {
            return true;
        }
        throw new UnsupportedOperationException("getFeature("+name+")");
    }

    @Override
    public void setFeature(String name, boolean value) throws SAXNotRecognizedException, SAXNotSupportedException {
        logger.trace("ignoring setFeature("+name+", "+value+")");
    }

    @Override
    public Object getProperty(String name) throws SAXNotRecognizedException, SAXNotSupportedException {
        if (name.equals("http://xml.org/sax/properties/lexical-handler")) {
            return lh;
        }
        throw new UnsupportedOperationException("getProperty("+name+")");
    }

    @Override
    public void setProperty(String name, Object value) throws SAXNotRecognizedException, SAXNotSupportedException {
        if (name.equals("http://xml.org/sax/properties/lexical-handler")) {
            lh = (LexicalHandler)value;
        } else {
            throw new UnsupportedOperationException("setProperty("+name+", "+value+")");
        }
    }

    @Override
    public void setEntityResolver(EntityResolver resolver) {
        throw new UnsupportedOperationException("setEntityResolver("+resolver+")");
    }

    @Override
    public EntityResolver getEntityResolver() {
        throw new UnsupportedOperationException("getEntityResolver()");
    }

    @Override
    public void setDTDHandler(DTDHandler handler) {
        dh = handler;
    }

    @Override
    public DTDHandler getDTDHandler() {
        return dh;
    }

    @Override
    public void setContentHandler(ContentHandler handler) {
        ch = handler;
    }

    @Override
    public ContentHandler getContentHandler() {
        return ch;
    }

    @Override
    public void setErrorHandler(ErrorHandler handler) {
        eh = handler;
    }

    @Override
    public ErrorHandler getErrorHandler() {
        return eh;
    }

    private class LoggingErrorListener extends StandardErrorListener {

        ByteArrayOutputStream baos = null;

        private void resetErrorOutput() {
            baos = new ByteArrayOutputStream();
            setErrorOutput(new PrintStream(baos));
        }

        private String getErrorOutputString() {
            getErrorOutput().flush();
            return baos.toString();
        }

        @Override
        public void warning(TransformerException exception) throws TransformerException {
            resetErrorOutput();
            try {
                super.warning(exception);
            } finally {
                if (baos.size() > 0) {
                    logger.trace(getErrorOutputString());
                }
            }
        }

        @Override
        public void error(TransformerException exception) throws TransformerException {
            resetErrorOutput();
            try {
                super.error(exception);
            } finally {
                if (baos.size() > 0) {
                    logger.trace(getErrorOutputString());
                }
            }
        }

        @Override
        public void fatalError(TransformerException exception) throws TransformerException {
            resetErrorOutput();
            try {
                super.fatalError(exception);
            } finally {
                if (baos.size() > 0) {
                    logger.trace(getErrorOutputString());
                }
            }
        }

    }

    private boolean parsing = false;

    @Override
    public final void parse(InputSource input) throws IOException, SAXException {
        parsing = true;
        try {
            try {
                initializeResultSet();
            } catch (ConnectionException ex) {
                throw new IOException(ex);
            } catch (SQLException ex) {
                throw new IOException(ex);
            }
            try {
                outputResultSetAsSAXEvents();
            } catch (SQLException ex) {
                throw new IOException(ex);
            }
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException ex) {
                    logger.error("exception closing resultset", ex);
                }
            }
        }
        parsing = false;
    }
    
    private void initializeResultSet() throws ConnectionException, SQLException {
        logger.trace("initializing resultset");
        rs = connection.getResultSet();
        idFieldQNames = new String[idFieldLabels.length];
        lastId = new long[idFieldLabels.length];
        currentId = new long[idFieldLabels.length];
        for (int i = 0; i < idFieldQNames.length; i++) {
            idFieldQNames[i] = integratorPrefix+":"+idFieldLabels[i];
        }
        ResultSetMetaData rsmd = rs.getMetaData();
        if (outputFieldLabels == null) {
            int columnCount = rsmd.getColumnCount();
            outputFieldLabels = new String[columnCount - idFieldLabels.length + 1];
            outputFieldColumnTypes = new int[outputFieldLabels.length];
            ArrayList<String> idFieldLabelsList = new ArrayList<String>(Arrays.asList(idFieldLabels));
            idFieldLabelsList.remove(idFieldLabels[idFieldLabels.length - 1]);
            int outputFieldIndex = 0;
            for (int i = 1; i <= columnCount; i++) {
                String columnLabel = rsmd.getColumnLabel(i);
                if (!idFieldLabelsList.contains(columnLabel)) {
                    outputFieldColumnTypes[outputFieldIndex] = rsmd.getColumnType(i);
                    outputFieldLabels[outputFieldIndex++] = columnLabel;
                }
            }
            if (outputFieldIndex != outputFieldLabels.length) {
                throw new IllegalStateException("unexpected state of outputFieldLabels");
            }
        } else {
            outputFieldColumnTypes = new int[outputFieldLabels.length];
            for (int i = 0; i < outputFieldLabels.length; i++) {
                outputFieldColumnTypes[i] = rsmd.getColumnType(rs.findColumn(outputFieldLabels[i]));
            }
        }
    }

    @Override
    public void parse(String systemId) throws IOException, SAXException {
        throw new UnsupportedOperationException("parse(String "+systemId+")");
    }
    
    public void setHost(String host) throws ConnectionException {
        logger.trace("set host: " + host);
        if (connection == null) {
            connection = new Connection();
        }
        connection.setHost(host);
    }

    public void setSid(String sid) throws ConnectionException {
        logger.trace("set sid: " + sid);
        if (connection == null) {
            connection = new Connection();
        }
        connection.setSid(sid);
    }

    public void setSql(String sql) throws ConnectionException {
        logger.trace("set sql: " + sql);
        if (connection == null) {
            connection = new Connection();
        }
        connection.setSql(sql);
    }

    public void setUser(String user) throws ConnectionException {
        logger.trace("set user: " + user);
        if (connection == null) {
            connection = new Connection();
        }
        connection.setUser(user);
    }

    public void setPwd(String pwd) throws ConnectionException {
        logger.trace("set pwd: " + pwd);
        if (connection == null) {
            connection = new Connection();
        }
        connection.setPwd(pwd);
    }

    public static final String INTEGRATOR_URI = "http://integrator";
    private String integratorPrefix = "integ";

    private String[] idFieldLabels;
    private String[] idFieldQNames;
    private long[] lastId;
    private long[] currentId;
    private int idFieldDepth = -1;

    private AttributesImpl attRunner = new AttributesImpl();
    private String idAttLocalName = "id";
    private String idAttQName = integratorPrefix+":"+idAttLocalName;

    private BufferingXMLFilter endElementBuffer = new BufferingXMLFilter();
    private void writeStructuralEvents() throws SQLException, SAXException {
        for (int i = 0; i < currentId.length ; i++) {
            lastId[i] = currentId[i];
            currentId[i] = rs.getLong(idFieldLabels[i]);
        }
        int i = idFieldDepth + 1;
        int decreasedFieldDepthLimit = idFieldDepth;
        endElementBuffer.clear();
        boolean endElementBufferEmpty = true;
        while (--i > -1) {
            if (currentId[i] != lastId[i]) {
                if (!endElementBufferEmpty) {
                    idFieldDepth += endElementBuffer.flush(ch);
                    endElementBufferEmpty = true;
                }
                ch.endElement(INTEGRATOR_URI, idFieldLabels[i], idFieldQNames[i]);
                idFieldDepth--;
                decreasedFieldDepthLimit = i - 1;
            } else {
                endElementBuffer.endElement(INTEGRATOR_URI, idFieldLabels[i], idFieldQNames[i]);
                endElementBufferEmpty = false;
            }
        }
        i = decreasedFieldDepthLimit;
        while (++i < currentId.length) {
            attRunner.clear();
            attRunner.addAttribute(INTEGRATOR_URI, idAttLocalName, idAttQName, "CDATA", Long.toString(currentId[i]));
            idFieldDepth++;
            ch.startElement(INTEGRATOR_URI, idFieldLabels[i], idFieldQNames[i], attRunner);
        }
    }

    String[] outputFieldLabels;
    int[] outputFieldColumnTypes;

    private void outputResultSetAsSAXEvents() throws SQLException, SAXException {
        initializeOutput();
        while (rs.next()) {
            writeStructuralEvents();
            for (int i = 0; i < outputFieldLabels.length; i++) {
                Object content = null;
                try {
                    switch (outputFieldColumnTypes[i]) {
                        case Types.NUMERIC:
                            if (expectedInputImplementation == Reader.class) {
                                content = new StringReader(Long.toString(rs.getLong(outputFieldLabels[i])));
                            } else if (expectedInputImplementation == InputStream.class) {
                                content = new ByteArrayInputStream(Long.toString(rs.getLong(outputFieldLabels[i])).getBytes());
                            }
                            break;
                        case Types.DATE:
                            String dateString = rs.getString(outputFieldLabels[i]);
                            if (dateString != null) {
                                if (expectedInputImplementation == Reader.class) {
                                    content = new StringReader(dateString);
                                } else if (expectedInputImplementation == InputStream.class) {
                                    content = new ByteArrayInputStream(dateString.getBytes());
                                }
                            }
                            break;
                        default:
                            if (expectedInputImplementation == Reader.class) {
                                content = rs.getCharacterStream(outputFieldLabels[i]);
                            } else if (expectedInputImplementation == InputStream.class) {
                                content = rs.getBinaryStream(outputFieldLabels[i]);
                            }
                    }
                } catch (SQLException ex) {
                    throw new RuntimeException("add special handling for java.sql.Types="+outputFieldColumnTypes[i],ex);
                }
                try {
                    outputFieldAsSAXEvents(currentId[currentId.length - 1], outputFieldLabels[i], content);
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                } finally {
                    try {
                        if (content != null) {
                            if (content instanceof Reader) {
                                ((Reader) content).close();
                            } else if (content instanceof InputStream) {
                                ((InputStream) content).close();
                            }
                        }
                    } catch (IOException ex) {
                        logger.error(ex);
                    }
                }
            }
        }
        finalizeOutput();
    }

    private String rootElementName = "root";

    private void initializeOutput() throws SAXException {
        ch.startDocument();
        ch.startPrefixMapping(integratorPrefix, INTEGRATOR_URI);
        ch.startElement(INTEGRATOR_URI, rootElementName, integratorPrefix+":"+rootElementName, attRunner);
    }

    private void finalizeOutput() throws SAXException, SQLException {
        int i = idFieldDepth + 1;
        while (--i > -1) {
            ch.endElement(INTEGRATOR_URI, idFieldLabels[i], idFieldQNames[i]);
            idFieldDepth--;
        }
        ch.endElement(INTEGRATOR_URI, rootElementName, integratorPrefix+":"+rootElementName);
        ch.endPrefixMapping(integratorPrefix);
        ch.endDocument();
    }

    protected abstract void outputFieldAsSAXEvents(long selfId, String fieldLabel, Object content) throws SAXException, IOException;


}
