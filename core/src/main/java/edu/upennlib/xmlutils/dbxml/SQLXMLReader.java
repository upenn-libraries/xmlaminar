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

import edu.upennlib.xmlutils.BoundedXMLFilterBuffer;
import edu.upennlib.xmlutils.SAXFeatures;
import edu.upennlib.xmlutils.SAXProperties;
import edu.upennlib.xmlutils.UnboundedContentHandlerBuffer;
import edu.upennlib.xmlutils.VolatileXMLFilterImpl;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.text.ParseException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.sql.DataSource;
import oracle.jdbc.OracleConnection;
import oracle.jdbc.pool.OracleConnectionPoolDataSource;
import org.xml.sax.ContentHandler;
import org.xml.sax.DTDHandler;
import org.xml.sax.EntityResolver;
import org.xml.sax.ErrorHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXNotSupportedException;
import org.apache.log4j.Logger;
import org.xml.sax.ext.LexicalHandler;
import org.xml.sax.helpers.AttributesImpl;

/**
 *
 * @author michael
 */
public abstract class SQLXMLReader extends VolatileXMLFilterImpl {

    public static final String TRANSFORMER_FACTORY_CLASS_NAME = "net.sf.saxon.TransformerFactoryImpl";
    public static final int DEFAULT_CHUNK_SIZE = 6;
    private String name;
    private int chunkSize;
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
    private DataSource ds;
    private String dataSourceName;
    private DataSourceFactory dsf;
    
    public DataSource getDataSource() {
        return ds;
    }
    
    public void setDataSource(DataSource ds) {
        this.ds = ds;
    }
    
    public String getDataSourceName() {
        return dataSourceName;
    }
    
    public void setDataSourceName(String name) {
        if (name == null ? this.dataSourceName != null : !name.equals(this.dataSourceName)) {
            this.dataSourceName = name;
            if (compiledSql != null && dsf != null) {
                ds = dsf.newDataSource(dataSourceName, compiledSql);
            }
        }
    }
    
    public static DataSource newDataSource(File connectionProps) {
        try {
            OracleConnectionPoolDataSource ds = (OracleConnectionPoolDataSource) Class.forName("oracle.jdbc.pool.OracleConnectionPoolDataSource").newInstance();
            ds.setDriverType("thin");
            ds.setPortNumber(1521);
            Properties cProps = new Properties();
            cProps.load(new FileReader(connectionProps));
            ds.setServerName((String) cProps.remove("server"));
            ds.setDatabaseName((String) cProps.remove("database"));
            ds.setConnectionProperties(cProps);
            return ds;
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        } catch (ClassNotFoundException ex) {
            throw new RuntimeException(ex);
        } catch (InstantiationException ex) {
            throw new RuntimeException(ex);
        } catch (IllegalAccessException ex) {
            throw new RuntimeException(ex);
        }
    }

    protected static enum InputImplementation { CHAR_ARRAY, BYTE_ARRAY };

    static {
        unmodifiableFeaturesAbs.put(SAXFeatures.NAMESPACES, true);
        unmodifiableFeaturesAbs.put(SAXFeatures.NAMESPACE_PREFIXES, false);
        unmodifiableFeaturesAbs.put(SAXFeatures.VALIDATION, false);
        
        featureDefaults.put(SAXFeatures.STRING_INTERNING, true);
    }

    protected SQLXMLReader(InputImplementation expectedInputImplementation, Map<String, Boolean> unmodifiableFeatures) {
        this(expectedInputImplementation, unmodifiableFeatures, DEFAULT_CHUNK_SIZE, DEFAULT_LOOKAHEAD_FACTOR);
    }

    private static final int DEFAULT_LOOKAHEAD_FACTOR = 0;
    private final int rsQueueLength;
    
    protected SQLXMLReader(InputImplementation expectedInputImplementation, Map<String, Boolean> unmodifiableFeatures, int chunkSize, int lookaheadFactor) {
        setBatchSizeLocal(chunkSize);
        this.rsQueueLength = lookaheadFactor + 1;
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
        psQueue = new ArrayBlockingQueue<StatementEnqueuer>(rsQueueLength);
        if (rsQueueLength > 1) {
            rsQueue = new ConcurrentHashMap<String, StatementEnqueuer>(rsQueueLength);
            startIds = new LinkedBlockingDeque<String>(rsQueueLength);
        } else {
            rsQueue = null;
            startIds = null;
        }
        for (int i = 0; i < rsQueueLength; i++) {
            psQueue.add(new StatementEnqueuer());
        }
    }

    public int getBatchSize() {
        return chunkSize;
    }
    
    public void setBatchSize(int size) {
        setBatchSizeLocal(size);
    }
    
    private void setBatchSizeLocal(int size) {
        if (size < 1) {
            throw new IllegalArgumentException("size="+size+"; must be > 0");
        }
        this.chunkSize = size;
        compiledSql = null;
    }

    public PerformanceEvaluator getPerformanceEvaluator() {
        return pe;
    }

    public void setPerformanceEvaluator(PerformanceEvaluator pe) {
        this.pe = pe;
    }

    public void setName(String name) {
        this.name = name;
    }

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
            lh = (LexicalHandler) value;
        } else if (SAXProperties.EXECUTOR_SERVICE_PROPERTY_NAME.equals(name)) {
            if (getExecutor() == null) {
                setExecutor((ExecutorService) value);
            }
        } else if (SAXProperties.DATA_SOURCE_FACTORY_PROPERTY_NAME.equals(name)) {
            setDataSourceFactory((DataSourceFactory) value);
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
    
    private Iterator<String> paramIter;
    
    public void setQueryParams(Iterator<String> paramIter) {
        this.paramIter = paramIter;
    }

    private static class InputSourceIterator implements Iterator<String> {

        private final Scanner s;
        private boolean done = false;

        private InputSourceIterator(InputSource in, Pattern inputDelimPattern) {
            Reader r;
            InputStream stream;
            String systemId;
            if ((r = in.getCharacterStream()) != null) {
                s = new Scanner(r);
            } else if ((stream = in.getByteStream()) != null) {
                s = new Scanner(stream);
            } else if ((systemId = in.getSystemId()) != null) {
                try {
                    s = new Scanner(new File(systemId));
                } catch (FileNotFoundException ex) {
                    throw new RuntimeException(ex);
                }
            } else {
                throw new RuntimeException("input source contains no information");
            }
            s.useDelimiter(inputDelimPattern);
        }

        @Override
        public boolean hasNext() {
            if (done) {
                return false;
            } else if (s.hasNext()) {
                return true;
            } else {
                s.close();
                done = true;
                return false;
            }
        }

        @Override
        public String next() {
            return s.next();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Not supported.");
        }
        
    }
    
    private final Map<String, StatementEnqueuer> rsQueue;
    private final BlockingQueue<StatementEnqueuer> psQueue;
    private final LinkedBlockingDeque<String> startIds;
    
    private class StatementEnqueuer implements Runnable {

        private volatile boolean complete = false;
        private ResultSet rs;
        private String startId;
        private Connection connection;
        private PreparedStatement ps;
        private SQLException ex;
        private RuntimeException rex;
        private Error e;
        private final Lock rsLock = new ReentrantLock();
        private final Condition hasRs = rsLock.newCondition();

        private void reset() {
            rs = null;
            startId = null;
            ex = null;
            rex = null;
            e = null;
            complete = false;
        }
        
        public String init(Iterator<String> paramIter, String lastStartId) throws SQLException {
            PSInitStruct psInit = initializePreparedStatement(paramIter, lastStartId);
            connection = psInit.c;
            ps = psInit.ps;
            startId = psInit.startId;
            return startId;
        }

        public Entry<ResultSet, Connection> getResultSet(Iterator<String> paramIter) throws SQLException {
            Entry<ResultSet, Connection> ret = null;
            try {
                if (!complete) {
                    rsLock.lock();
                    try {
                        while (!complete) {
                            hasRs.await();
                        }
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    } finally {
                        rsLock.unlock();
                    }
                }
                if (ex != null) {
                    throw ex;
                } else if (rex != null) {
                    throw rex;
                } else if (e != null) {
                    throw e;
                }
                if (paramIter != null) {
                    if (!paramIter.hasNext()) {
                        throw new IllegalStateException();
                    }
                    String compareStartId = paramIter.next();
                    if (!compareStartId.equals(startId)) {
                        throw new IllegalStateException();
                    }
                    int i = 1;
                    while (paramIter.hasNext() && i++ < chunkSize) {
                        paramIter.next();
                    }
                }
                ret = rs == null ? null : new AbstractMap.SimpleEntry<ResultSet, Connection>(rs, connection);
                reset();
                return ret;
            } finally {
                if (ret == null) {
                    connection.close();
                }
            }
        }
        
        @Override
        public void run() {
            boolean successful = false;
            try {
                rs = ps.executeQuery();
                successful = true;
            } catch (SQLException ex) {
                this.ex = ex;
            } catch (RuntimeException ex) {
                this.rex = ex;
            } catch (Error ex) {
                this.e = ex;
            } finally {
                rsLock.lock();
                complete = true;
                try {
                    hasRs.signal();
                } finally {
                    rsLock.unlock();
                }
                if (!successful) {
                    try {
                        connection.close();
                    } catch (SQLException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }
        }
        
    }
    
    private boolean suppressParameterizedClause = false;
    
    public void setSuppressParameterizedClause(boolean suppress) {
        if (this.suppressParameterizedClause != suppress) {
            compiledSql = null;
            this.suppressParameterizedClause = suppress;
        }
    }
    
    public boolean isSuppressParameterizedClause() {
        return suppressParameterizedClause;
    }
    
    private Pattern inputDelimPattern = Pattern.compile(System.lineSeparator(), Pattern.LITERAL);
    
    public void setInputDelimPattern(String inputDelim) {
        inputDelimPattern = Pattern.compile(inputDelim, Pattern.LITERAL);
    }
    
    public String getInputDelimPattern() {
        return inputDelimPattern == null ? null : inputDelimPattern.pattern();
    }
    
    private ExecutorService executor;
    
    public ExecutorService getExecutor() {
        return executor;
    }
    
    public void setExecutor(ExecutorService executor) {
        this.executor = executor;
        buffer.setExecutor(executor);
    }
    
    public DataSourceFactory getDataSourceFactory() {
        return dsf;
    }
    
    public void setDataSourceFactory(DataSourceFactory dsf) {
        if (this.dsf != dsf) {
            this.dsf = dsf;
            if (compiledSql != null && dataSourceName != null) {
                ds = dsf.newDataSource(dataSourceName, compiledSql);
            }
        }
    }
    
    @Override
    public final void parse(InputSource input) throws IOException, SAXException {
        if (!parsing) {
            parsing = true;
            attRunner.clear();
            buffer.clear();
            buffer.setParent(this);
            buffer.setContentHandler(ch);
            if (compiledSql == null) {
                try {
                    compileSQL();
                } catch (ParseException ex) {
                    throw new RuntimeException(ex);
                }
            }
            if (parameterizedSQL && paramIter == null) {
                paramIter = new InputSourceIterator(input, inputDelimPattern);
            }
            buffer.parse(input);
        } else {
            Connection c = null;
            try {
                StatementEnqueuer se;
                StatementEnqueuer direct = null;
                String startId;
                try {
                    if (rsQueueLength > 1 && parameterizedSQL && (startId = startIds.poll()) != null) {
                        direct = rsQueue.get(startId);
                        Entry<ResultSet, Connection> rc = direct.getResultSet(paramIter);
                        rs = rc.getKey();
                        c = rc.getValue();
                    } else {
                        direct = psQueue.remove();
                        direct.init(paramIter, null);
                        direct.run();
                        Entry<ResultSet, Connection> rc = direct.getResultSet(null);
                        rs = rc.getKey();
                        c = rc.getValue();
                    }
                    if (rsQueueLength > 1 && parameterizedSQL && paramIter.hasNext() && (se = psQueue.poll()) != null) {
                        startId = se.init(paramIter, startIds.peekLast());
                        if (startId == null) {
                            psQueue.add(se);
                        } else {
                            startIds.add(startId);
                            rsQueue.put(startId, se);
                            executor.submit(se);
                        }
                    }
                } finally {
                    if (direct != null) {
                        psQueue.add(direct);
                    }
                }
                try {
                    initializeResultSet();
                } catch (SQLException ex) {
                    throw new IOException(ex);
                }
                try {
                    outputResultSetAsSAXEvents();
                } catch (SQLException ex) {
                    throw new IOException(ex);
                }
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            } finally {
                paramIter = null;
                rs = null;
                if (c != null) {
                    try {
                        c.close();
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

    private static class PSInitStruct {
        public final String startId;
        public final PreparedStatement ps;
        public final Connection c;

        public PSInitStruct(String startId, PreparedStatement ps, Connection c) {
            this.startId = startId;
            this.ps = ps;
            this.c = c;
        }
        
    }
    
    private SQLParam repeatParamType = null;
    private final List<SQLParam> paramTypes = new ArrayList<SQLParam>();
    
    private PSInitStruct initializePreparedStatement(Iterator<String> paramIter, String precedingStartId) throws SQLException {
        Connection connection;
        PreparedStatement ps;
        connection = ds.getConnection();
        boolean initSuccessful = false;
        try {
            ps = connection.prepareStatement(compiledSql);
            if (!parameterizedSQL || !paramIter.hasNext()) {
                initSuccessful = true;
                return new PSInitStruct(null, ps, connection);
            }
            boolean output = precedingStartId == null;
            do {
                String startId = paramIter.next();
                if (output) {
                    Iterator<SQLParam> spIter;
                    if (repeatParamType != null) {
                        spIter = new RepeatIterator<SQLParam>(repeatParamType);
                    } else {
                        spIter = paramTypes.iterator();
                    }
                    spIter.next().init(ps, 1, startId);
                    String val = startId;
                    for (int i = 2; i <= chunkSize; i++) {
                        if (paramIter.hasNext()) {
                            val = paramIter.next();
                        }
                        spIter.next().init(ps, i, val);
                    }
                    initSuccessful = true;
                    return new PSInitStruct(startId, ps, connection);
                } else {
                    output = precedingStartId.equals(startId);
                    for (int i = 1; i < chunkSize && paramIter.hasNext(); i++) {
                        paramIter.next();
                    }
                }
            } while (paramIter.hasNext());
            return null;
        } finally {
            if (!initSuccessful) {
                connection.close();
            }
        }
    }

    private static class RepeatIterator<T> implements Iterator<T> {

        private final T val;
        
        private RepeatIterator(T val) {
            this.val = val;
        }
        
        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public T next() {
            return val;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Not supported.");
        }
        
    }
    
    private void initializeResultSet() throws SQLException {
        logger.trace("initializing resultset");
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

    private String compiledSql;
    
    public void setSql(String sql) {
        logger.trace("set sql: " + sql);
        this.sql = sql;
        compiledSql = null;
    }

    private void compileSQL() throws ParseException {
        String parameterized = parameterizeSQL(sql, chunkSize);
        if (parameterized != null) {
            parameterizedSQL = !suppressParameterizedClause;
            this.compiledSql = parameterized;
        } else {
            parameterizedSQL = false;
            this.compiledSql = sql;
        }
        if (dsf != null && dataSourceName != null) {
            ds = dsf.newDataSource(dataSourceName, compiledSql);
        }
    }

    public boolean isParameterized() {
        if (compiledSql == null) {
            try {
                compileSQL();
            } catch (ParseException ex) {
                throw new RuntimeException(ex);
            }
        }
        return parameterizedSQL;
    }
    
    private boolean parameterizedSQL = false;

    private static final String SUPPORTED_PARAM_TYPES;
    
    private static final Pattern PARAM_CLAUSE_DELIMS = Pattern.compile("(<\\\\[^\\\\])|([^\\\\]\\\\>)");    
    
    private String parameterizeSQL(String sql, int chunkSize) throws ParseException {
        Matcher m = PARAM_CLAUSE_DELIMS.matcher(sql);
        repeatParamType = null;
        paramTypes.clear();
        StringBuilder sb = new StringBuilder();
        int depth = 0;
        boolean lastWasStart = false;
        int contentOffset = 0;
        if (!m.find()) {
            return null;
        } else {
            do {
                if (m.group(1) != null) {
                    lastWasStart = true;
                    if (depth++ < 1 || !suppressParameterizedClause) {
                        sb.append(sql, contentOffset, m.start());
                    }
                    contentOffset = m.end() - 1;
                } else {
                    if (--depth < 0) {
                        throw new ParseException(sql, m.start());
                    }
                    if (!suppressParameterizedClause) {
                        int endContent = m.start() + 1;
                        Matcher m1;
                        if (lastWasStart && (m1 = PARAM_PATTERN.matcher(sql.substring(contentOffset, endContent))).matches()) {
                            sb.append('?');
                            String typeName = m1.group(1);
                            boolean repeatParam = m1.group(2) != null;
                            SQLParam paramType = SQLParam.valueOf(typeName);
                            if (!repeatParam) {
                                if (repeatParamType != null) {
                                    throw new IllegalArgumentException("may not mix individual param "+repeatParamType+" with repeatable param "+paramType);
                                }
                                paramTypes.add(paramType);
                            } else {
                                if (repeatParamType == null) {
                                    repeatParamType = paramType;
                                } else if (repeatParamType != paramType) {
                                    throw new IllegalArgumentException("multiple specifications of repeatable params must have identical type; "
                                            +repeatParamType +" != "+paramType);
                                } else if (!paramTypes.isEmpty()) {
                                    throw new IllegalArgumentException("may not mix repeatble param "+paramType+" with individual params "+paramTypes);
                                }
                                for (int i = 1; i < chunkSize; i++) {
                                    sb.append(",?");
                                }
                            }
                        } else {
                            sb.append(sql, contentOffset, endContent);
                        }
                    }
                    lastWasStart = false;
                    contentOffset = m.end();
                }
            } while (m.find());
        }
        if (depth != 0) {
            throw new ParseException(sql, sql.length());
        }
        sb.append(sql, contentOffset, sql.length());
        if (repeatParamType == null && chunkSize != paramTypes.size()) {
            setBatchSize(paramTypes.size());
        }
        return sb.toString();
    }
    
    public static void main(String[] args) throws Exception {
        SQLXMLReader mxr = new BinaryMARCXMLReader();
        mxr.suppressParameterizedClause = true;
        String parameterized = mxr.parameterizeSQL("something <\\ and id in (<\\INTEGER*\\>)\\> else ", 6);
        System.out.println(parameterized);
    }
    
    static {
        StringBuilder sb = new StringBuilder();
        SQLParam[] vals = SQLParam.values();
        if (vals.length > 0) {
            sb.append('(').append(vals[0].toString());
            for (int i = 1; i < vals.length; i++) {
                sb.append('|').append(vals[i].toString());
            }
            sb.append(")(\\*)?");
        }
        SUPPORTED_PARAM_TYPES = sb.toString();
    }
    
    private static final Pattern PARAM_PATTERN = Pattern.compile(SUPPORTED_PARAM_TYPES);
    
    protected String parameterizeSQLOld(String sql, int chunkSize) {
        Matcher m = PARAM_PATTERN.matcher(sql);
        if (!m.find()) {
            repeatParamType = null;
            return null;
        } else {
            String typeName = m.group(1);
            boolean repeatParam = m.group(2) != null;
            repeatParamType = SQLParam.valueOf(typeName);
            StringBuffer sb = new StringBuffer(sql.length() - typeName.length() + (chunkSize * 2));
            sb.append('?');
            if (repeatParam) {
                for (int i = 1; i < chunkSize; i++) {
                    sb.append(",?");
                }
            }
            String replacement = sb.toString();
            sb.setLength(0);
            do {
                m.appendReplacement(sb, replacement);
            } while (m.find());
            m.appendTail(sb);
            return sb.toString();
        }
    }

    public String getSql() {
	    return sql;
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
