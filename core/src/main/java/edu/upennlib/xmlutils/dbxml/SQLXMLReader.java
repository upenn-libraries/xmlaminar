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

package edu.upennlib.xmlutils.dbxml;

import edu.upennlib.configurationutils.IndexedPropertyConfigurable;
import edu.upennlib.dbutils.Connection;
import edu.upennlib.dbutils.ConnectionException;
import edu.upennlib.dbutils.DirectConnection;
import edu.upennlib.xmlutils.BoundedXMLFilterBuffer;
import edu.upennlib.xmlutils.SAXFeatures;
import edu.upennlib.xmlutils.UnboundedContentHandlerBuffer;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
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
public abstract class SQLXMLReader implements XMLReader, IndexedPropertyConfigurable {

    public static final String TRANSFORMER_FACTORY_CLASS_NAME = "net.sf.saxon.TransformerFactoryImpl";
    private String name;
    private Connection connection;
    private String sql;
    private static final Logger logger = Logger.getLogger(SQLXMLReader.class);
    private ResultSet rs;
    protected ContentHandler ch;
    private final BoundedXMLFilterBuffer buffer = new BoundedXMLFilterBuffer();
    private ErrorHandler eh;
    private DTDHandler dh;
    private LexicalHandler lh;
    private final InputImplementation expectedInputImplementation;
    private PerformanceEvaluator pe;
    private static final HashMap<String, Boolean> unmodifiableFeaturesAbs = new HashMap<String, Boolean>();
    private static final HashMap<String, Boolean> featureDefaults = new HashMap<String, Boolean>();
    private final HashMap<String, Boolean> features = new HashMap<String, Boolean>();
    private final HashMap<String, Boolean> unmodifiableFeatures = new HashMap<String, Boolean>();
    
    public void setConnection(Connection connection) {
        this.connection = connection;
    }
    
    public Connection getConnection() {
        return connection;
    }

    protected static enum InputImplementation { CHAR_ARRAY, BYTE_ARRAY };

    static {
        unmodifiableFeaturesAbs.put(SAXFeatures.NAMESPACES, true);
        unmodifiableFeaturesAbs.put(SAXFeatures.NAMESPACE_PREFIXES, false);
        unmodifiableFeaturesAbs.put(SAXFeatures.VALIDATION, false);
        
        featureDefaults.put(SAXFeatures.STRING_INTERNING, true);
    }

    protected SQLXMLReader(InputImplementation expectedInputImplementation, Map<String, Boolean> unmodifiableFeatures) {
        this.expectedInputImplementation = expectedInputImplementation;
        for (Entry<String, Boolean> e : unmodifiableFeatures.entrySet()) {
            if (unmodifiableFeaturesAbs.containsKey(e.getKey())) {
                if (unmodifiableFeaturesAbs.get(e.getKey()) != e.getValue()) {
                    throw new IllegalArgumentException();
                }
            } else if (!featureDefaults.containsKey(e.getKey())) {
                throw new IllegalArgumentException();
            }
        }
        this.unmodifiableFeatures.putAll(unmodifiableFeaturesAbs);
        this.unmodifiableFeatures.putAll(unmodifiableFeatures);
        features.putAll(featureDefaults);
        features.keySet().removeAll(this.unmodifiableFeatures.keySet());
    }

    public PerformanceEvaluator getPerformanceEvaluator() {
        return pe;
    }

    public void setPerformanceEvaluator(PerformanceEvaluator pe) {
        this.pe = pe;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }

    public void setIdFieldLabels(String[] idFieldLabels) {
        for (int i = 0; i < idFieldLabels.length; i++) {
            idFieldLabels[i] = idFieldLabels[i].intern();
        }
        this.idFieldLabels = idFieldLabels;
    }

    public String[] getIdFieldLabels() {
        return idFieldLabels;
    }

    public void setOutputFieldLabels(String[] outputFieldLabels) {
        for (int i = 0; i < outputFieldLabels.length; i++) {
            outputFieldLabels[i] = outputFieldLabels[i].intern();
        }
        this.outputFieldLabels = outputFieldLabels;
    }

    public String[] getOutputFieldLabels() {
        return outputFieldLabels;
    }

    @Override
    public boolean getFeature(String name) throws SAXNotRecognizedException, SAXNotSupportedException {
        if (unmodifiableFeatures.containsKey(name)) {
            return unmodifiableFeatures.get(name);
        } else if (features.containsKey(name)) {
            return features.get(name);
        } else {
            throw new SAXNotRecognizedException(name);
        }
    }

    @Override
    public void setFeature(String name, boolean value) throws SAXNotRecognizedException, SAXNotSupportedException {
        if (unmodifiableFeatures.containsKey(name)) {
            if (unmodifiableFeatures.get(name) != value) {
                throw new SAXNotSupportedException(this+" does not support setting feature "+name+" to "+ value);
            }
        } else if (features.containsKey(name)) {
            features.put(name, value);
        } else {
            throw new SAXNotRecognizedException(name);
        }
    }

    @Override
    public Object getProperty(String name) throws SAXNotRecognizedException, SAXNotSupportedException {
        if (name.equals("http://xml.org/sax/properties/lexical-handler")) {
            return lh;
        }
        throw new SAXNotRecognizedException("getProperty("+name+")");
    }

    @Override
    public void setProperty(String name, Object value) throws SAXNotRecognizedException, SAXNotSupportedException {
        if (name.equals("http://xml.org/sax/properties/lexical-handler")) {
            lh = (LexicalHandler)value;
        } else {
            throw new SAXNotRecognizedException("setProperty("+name+", "+value+")");
        }
    }

    @Override
    public void setEntityResolver(EntityResolver resolver) {
        logger.trace("ignoring setEntityResolver(" + resolver + ")");
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

    private boolean parsing = false;

    @Override
    public final void parse(InputSource input) throws IOException, SAXException {
        if (!parsing) {
            parsing = true;
            buffer.clear();
            buffer.setParent(this);
            buffer.setContentHandler(ch);
            buffer.parse(input);
        } else {
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
                connection.close();

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
    }

    //@Override
    public final void parseOld(InputSource input) throws IOException, SAXException {
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
        connection.setSql(sql);
        rs = connection.getResultSet();
        if (pe != null) {
            pe.notifyStart();
        }
        idFieldQNames = new String[idFieldLabels.length];
        lastId = new long[idFieldLabels.length];
        currentId = new long[idFieldLabels.length];
        for (int i = 0; i < idFieldQNames.length; i++) {
            idFieldQNames[i] = (INTEGRATOR_PREFIX+":"+idFieldLabels[i]).intern();
        }
        ResultSetMetaData rsmd = rs.getMetaData();
        if (outputFieldLabels == null) {
            int columnCount = rsmd.getColumnCount();
            outputFieldLabels = new String[columnCount - idFieldLabels.length + 1];
            outputFieldLabelsLower = new String[columnCount - idFieldLabels.length + 1];
            outputFieldColumnTypes = new int[outputFieldLabels.length];
            ArrayList<String> idFieldLabelsList = new ArrayList<String>(Arrays.asList(idFieldLabels));
            idFieldLabelsList.remove(idFieldLabels[idFieldLabels.length - 1]);
            int outputFieldIndex = 0;
            for (int i = 1; i <= columnCount; i++) {
                String columnLabel = rsmd.getColumnLabel(i);
                if (!idFieldLabelsList.contains(columnLabel)) {
                    outputFieldColumnTypes[outputFieldIndex] = rsmd.getColumnType(i);
                    outputFieldLabels[outputFieldIndex] = columnLabel;
                    outputFieldLabelsLower[outputFieldIndex++] = columnLabel.toLowerCase().intern();
                }
            }
            if (outputFieldIndex != outputFieldLabels.length) {
                throw new IllegalStateException("unexpected state of outputFieldLabels");
            }
        } else {
            outputFieldColumnTypes = new int[outputFieldLabels.length];
            outputFieldLabelsLower = new String[outputFieldLabels.length];
            for (int i = 0; i < outputFieldLabels.length; i++) {
                outputFieldColumnTypes[i] = rsmd.getColumnType(rs.findColumn(outputFieldLabels[i]));
                outputFieldLabelsLower[i] = outputFieldLabels[i].toLowerCase();
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
            connection = new DirectConnection();
        }
        connection.setHost(host);
    }

    public String getHost() {
        if (connection == null) {
            return null;
        } else {
            return connection.getHost();
        }
    }

    public void setSid(String sid) throws ConnectionException {
        logger.trace("set sid: " + sid);
        if (connection == null) {
            connection = new DirectConnection();
        }
        connection.setSid(sid);
    }
    
    public String getSid() {
        if (connection == null) {
            return null;
        } else {
            return connection.getSid();
        }
    }

    public void setSql(String sql) throws ConnectionException {
        logger.trace("set sql: " + sql);
		this.sql = sql;
    }

    public String getSql() {
	    return sql;
    }

    public void setUser(String user) throws ConnectionException {
        logger.trace("set user: " + user);
        if (connection == null) {
            connection = new DirectConnection();
        }
        connection.setUser(user);
    }

    public String getUser() {
        if (connection == null) {
            return null;
        } else {
            return connection.getUser();
        }
    }

    public void setPwd(String pwd) throws ConnectionException {
        logger.trace("set pwd: " + pwd);
        if (connection == null) {
            connection = new DirectConnection();
        }
        connection.setPwd(pwd);
    }

    public String getPwd() {
        if (connection == null) {
            return null;
        } else {
            return connection.getPwd();
        }
    }

    public static final String INTEGRATOR_URI = "http://integrator";
    public static final String INTEGRATOR_PREFIX = "integ";
    private static final String ROOT_ELEMENT_NAME = "root";
    public static final String INTEGRATOR_ROOT_QNAME = (INTEGRATOR_PREFIX+":"+ROOT_ELEMENT_NAME).intern();

    private String[] idFieldLabels;
    private String[] idFieldQNames;
    private long[] lastId;
    private long[] currentId;
    private int idFieldDepth = -1;

    private final AttributesImpl attRunner = new AttributesImpl();
    private final String ID_ATT_NAME = "id";
    private final String SELF_ATT_NAME = "self";

    private final UnboundedContentHandlerBuffer endElementBuffer = new UnboundedContentHandlerBuffer();
    private void writeStructuralEvents() throws SQLException, SAXException {
        System.arraycopy(currentId, 0, lastId, 0, currentId.length);
        for (int i = 0; i < currentId.length; i++) {
            //lastId[i] = currentId[i];
            currentId[i] = rs.getLong(idFieldLabels[i]);
        }
        int i = idFieldDepth + 1;
        int decreasedFieldDepthLimit = idFieldDepth;
        endElementBuffer.clear();
        boolean endElementBufferEmpty = true;
        while (--i > -1) {
            if (currentId[i] != lastId[i]) {
                if (!endElementBufferEmpty) {
                    idFieldDepth += endElementBuffer.flush(ch, null);
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
            attRunner.addAttribute("", ID_ATT_NAME, ID_ATT_NAME, "CDATA", Long.toString(currentId[i]));
            if (i == currentId.length - 1) {
                attRunner.addAttribute("", SELF_ATT_NAME, SELF_ATT_NAME, "CDATA", "true");
            }
            idFieldDepth++;
            ch.startElement(INTEGRATOR_URI, idFieldLabels[i], idFieldQNames[i], attRunner);
        }
    }

    private String[] outputFieldLabels;
    private String[] outputFieldLabelsLower;
    private int[] outputFieldColumnTypes;
    private static final int CHAR_BUFFER_SIZE_INIT = 2048;
    private char[] charBuffer = new char[CHAR_BUFFER_SIZE_INIT];

    private int populateCharBuffer(String s) {
        if (s == null) {
            return 0;
        }
        int length = s.length();
        while (charBuffer.length < length) {
            charBuffer = new char[charBuffer.length * 2];
        }
        s.getChars(0, length, charBuffer, 0);
        return length;
    }

    //Arrays
    private void outputResultSetAsSAXEventsArrays() throws SQLException, SAXException {
        initializeOutput();
        while (rs.next()) {
            writeStructuralEvents();
            for (int i = 0; i < outputFieldLabels.length; i++) {
                int readerEndIndex = -1;
                byte[] binaryContent = null;
                try {
                    switch (expectedInputImplementation) {
                        case CHAR_ARRAY:
                            readerEndIndex = populateCharBuffer(rs.getString(outputFieldLabels[i]));
                            break;
                        case BYTE_ARRAY:
                            binaryContent = rs.getBytes(outputFieldLabels[i]);
                    }
                } catch (SQLException ex) {
                    throw new RuntimeException("add special handling for java.sql.Types=" + outputFieldColumnTypes[i], ex);
                }
                try {
                    switch (expectedInputImplementation) {
                        case CHAR_ARRAY:
                            outputFieldAsSAXEvents(currentId[currentId.length - 1], outputFieldLabelsLower[i], charBuffer, readerEndIndex);
                            break;
                        case BYTE_ARRAY:
                            outputFieldAsSAXEvents(currentId[currentId.length - 1], outputFieldLabelsLower[i], binaryContent);
                    }
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            }
        }
        finalizeOutput();
    }

    /*private final int REPORT_INTERVAL = 201;
    private int lastReportedAgo = 0;
    private double avgResponseTime = 0;
    private long numberReports = 0;
    private boolean statsRsNext() throws SQLException {
        boolean next;
        if (++lastReportedAgo >= REPORT_INTERVAL) {
            long start = System.currentTimeMillis();
            next = rs.next();
            long responseTime = System.currentTimeMillis() - start;
            avgResponseTime = avgResponseTime + ((responseTime - avgResponseTime) / ++numberReports);
            lastReportedAgo = 0;
        } else {
            next = rs.next();
        }
        if (!next) {
            System.out.println(getName()+" avg="+avgResponseTime+"; total="+(REPORT_INTERVAL * numberReports + lastReportedAgo) * avgResponseTime);
        }
        return next;
    }

    public long rsNextEstimate() {
        double result = ((REPORT_INTERVAL * numberReports) + lastReportedAgo) * avgResponseTime;
        return (long) result;
    }*/

    //Hybrid
    private void outputResultSetAsSAXEventsHybrid() throws SQLException, SAXException {
        initializeOutput();
        while (rs.next()) {
            writeStructuralEvents();
            for (int i = 0; i < outputFieldLabels.length; i++) {
                Reader readerContent = null;
                byte[] binaryContent = null;
                try {
                    switch (outputFieldColumnTypes[i]) {
                        case Types.NUMERIC:
                            switch (expectedInputImplementation) {
                                case CHAR_ARRAY:
                                    readerContent = new StringReader(Long.toString(rs.getLong(outputFieldLabels[i])));
                                    break;
                                case BYTE_ARRAY:
                                    binaryContent = Long.toString(rs.getLong(outputFieldLabels[i])).getBytes();
                            }
                            break;
                        case Types.TIMESTAMP:
                        case Types.DATE:
                            String dateString = rs.getString(outputFieldLabels[i]);
                            if (dateString != null) {
                                switch (expectedInputImplementation) {
                                    case CHAR_ARRAY:
                                        readerContent = new StringReader(dateString);
                                        break;
                                    case BYTE_ARRAY:
                                        binaryContent = dateString.getBytes();
                                }
                            }
                            break;
                        default:
                            switch (expectedInputImplementation) {
                                case CHAR_ARRAY:
                                    readerContent = rs.getCharacterStream(outputFieldLabels[i]);
                                    break;
                                case BYTE_ARRAY:
                                    binaryContent = rs.getBytes(outputFieldLabels[i]);
                            }
                    }
                } catch (SQLException ex) {
                    throw new RuntimeException("add special handling for java.sql.Types=" + outputFieldColumnTypes[i], ex);
                }
                try {
                    switch (expectedInputImplementation) {
                        case CHAR_ARRAY:
                            outputFieldAsSAXEvents(currentId[currentId.length - 1], outputFieldLabelsLower[i], readerContent);
                            break;
                        case BYTE_ARRAY:
                            outputFieldAsSAXEvents(currentId[currentId.length - 1], outputFieldLabelsLower[i], binaryContent);
                    }
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                } finally {
                    if (expectedInputImplementation == InputImplementation.CHAR_ARRAY) {
                        if (readerContent != null) {
                            try {
                                readerContent.close();
                            } catch (IOException ex) {
                                logger.error(ex);
                            }
                        }
                    }
                }
            }
        }
        finalizeOutput();
    }

    //Streams
    private void outputResultSetAsSAXEvents() throws SQLException, SAXException {
        initializeOutput();
        while (rs.next()) {
            writeStructuralEvents();
            for (int i = 0; i < outputFieldLabels.length; i++) {
                Reader readerContent = null;
                InputStream binaryContent = null;
                try {
                    switch (outputFieldColumnTypes[i]) {
                        case Types.NUMERIC:
                            long val = rs.getLong(outputFieldLabels[i]);
                            if (!rs.wasNull()) {
                                String valString = Long.toString(val);
                                switch (expectedInputImplementation) {
                                    case CHAR_ARRAY:
                                        readerContent = new StringReader(valString);
                                        break;
                                    case BYTE_ARRAY:
                                        binaryContent = new ByteArrayInputStream(valString.getBytes());
                                }
                            }
                            break;
                        case Types.TIMESTAMP:
                        case Types.DATE:
                            String dateString = rs.getString(outputFieldLabels[i]);
                            if (dateString != null) {
                                switch (expectedInputImplementation) {
                                    case CHAR_ARRAY:
                                        readerContent = new StringReader(dateString);
                                        break;
                                    case BYTE_ARRAY:
                                        binaryContent = new ByteArrayInputStream(dateString.getBytes());
                                }
                            }
                            break;
                        default:
                            switch (expectedInputImplementation) {
                                case CHAR_ARRAY:
                                    readerContent = rs.getCharacterStream(outputFieldLabels[i]);
                                    break;
                                case BYTE_ARRAY:
                                    binaryContent = rs.getBinaryStream(outputFieldLabels[i]);
                            }
                    }
                } catch (SQLException ex) {
                    throw new RuntimeException("add special handling for java.sql.Types=" + outputFieldColumnTypes[i], ex);
                }
                try {
                    switch (expectedInputImplementation) {
                        case CHAR_ARRAY:
                            outputFieldAsSAXEvents(currentId[currentId.length - 1], outputFieldLabelsLower[i], readerContent);
                            break;
                        case BYTE_ARRAY:
                            outputFieldAsSAXEvents(currentId[currentId.length - 1], outputFieldLabelsLower[i], binaryContent);
                    }
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                } finally {
                    try {
                        switch (expectedInputImplementation) {
                            case CHAR_ARRAY:
                                if (readerContent != null) {
                                    readerContent.close();
                                }
                                break;
                            case BYTE_ARRAY:
                                if (binaryContent != null) {
                                    binaryContent.close();
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

    private void initializeOutput() throws SAXException {
        ch.startDocument();
        ch.startPrefixMapping(INTEGRATOR_PREFIX, INTEGRATOR_URI);
        ch.startElement(INTEGRATOR_URI, ROOT_ELEMENT_NAME, INTEGRATOR_ROOT_QNAME, attRunner);
    }

    private void finalizeOutput() throws SAXException, SQLException {
        int i = idFieldDepth + 1;
        while (--i > -1) {
            ch.endElement(INTEGRATOR_URI, idFieldLabels[i], idFieldQNames[i]);
            idFieldDepth--;
        }
        ch.endElement(INTEGRATOR_URI, ROOT_ELEMENT_NAME, INTEGRATOR_ROOT_QNAME);
        ch.endPrefixMapping(INTEGRATOR_PREFIX);
        ch.endDocument();
    }

    protected abstract void outputFieldAsSAXEvents(long selfId, String fieldLabel, char[] content, int endIndex) throws SAXException, IOException;
    protected abstract void outputFieldAsSAXEvents(long selfId, String fieldLabel, byte[] content) throws SAXException, IOException;

    protected abstract void outputFieldAsSAXEvents(long selfId, String fieldLabel, Reader content) throws SAXException, IOException;
    protected abstract void outputFieldAsSAXEvents(long selfId, String fieldLabel, InputStream content) throws SAXException, IOException;


}
