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

import static edu.upennlib.paralleltransformer.JoiningXMLFilter.GROUP_BY_SYSTEMID_FEATURE_NAME;
import edu.upennlib.paralleltransformer.callback.IncrementingFileCallback;
import edu.upennlib.paralleltransformer.callback.OutputCallback;
import edu.upennlib.paralleltransformer.callback.StaticFileCallback;
import edu.upennlib.paralleltransformer.callback.StdoutCallback;
import edu.upennlib.paralleltransformer.callback.XMLReaderCallback;
import edu.upennlib.xmlutils.LoggingErrorListener;
import edu.upennlib.xmlutils.UnboundedContentHandlerBuffer;
import edu.upennlib.xmlutils.VolatileSAXSource;
import edu.upennlib.xmlutils.VolatileXMLFilterImpl;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Level;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.Source;
import javax.xml.transform.Templates;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import net.sf.saxon.Controller;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.EntityResolver;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXNotSupportedException;
import org.xml.sax.XMLFilter;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLFilterImpl;

/**
 *
 * @author Michael Gibney
 */
public class TXMLFilter extends QueueSourceXMLFilter implements OutputCallback {

    private static final boolean DEFAULT_SUBDIVIDE = false;
    private final ProcessingQueue<Chunk> pq;
    private final Templates templates;
    private final boolean subdivide;
    private static final Logger LOG = LoggerFactory.getLogger(TXMLFilter.class);

    public TXMLFilter(Source xslSource, String xpath, boolean subdivide) throws TransformerConfigurationException {
        TransformerFactory tf = TransformerFactory.newInstance("net.sf.saxon.TransformerFactoryImpl", null);
        templates = tf.newTemplates(xslSource);
        pq = new ProcessingQueue<Chunk>(10, new Chunk(templates, xpath, subdivide));
        this.subdivide = subdivide;
    }
    
    public TXMLFilter(Source xslSource, String xpath) throws TransformerConfigurationException {
        this(xslSource, xpath, DEFAULT_SUBDIVIDE);
    }
    
    public static void main(String[] args) throws Exception {
        JoiningXMLFilter joiner = new JoiningXMLFilter(false);
        SplittingXMLFilter sxf = new LevelSplittingXMLFilter(1, 10);
        TXMLFilter txf = new TXMLFilter(new StreamSource("../cli/input/identity.xsl"), "/root/rec/@id", true);
        sxf.setOutputCallback(new StdoutCallback());
        joiner.setParent(txf);
//        ExecutorService ex = Executors.newCachedThreadPool();
//        sxf.setExecutor(ex);
        sxf.setParent(joiner);
        sxf.parse(new InputSource("../cli/var-length/50.xml"));
        System.out.println("one!");
        sxf.parse(new InputSource("../cli/var-length/25.xml"));
        System.out.println("Done!");
//        ex.shutdown();
    }
    
    @Override
    protected void initialParse(VolatileSAXSource in) {
        outputFuture = getExecutor().submit(new OutputRunnable(Thread.currentThread()));
        pq.reset();
        try {
            setupInputBuffer(in);
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    protected void repeatParse(VolatileSAXSource in) {
        try {
            setupInputBuffer(in);
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
    }
    
    @Override
    protected void finished() throws SAXException {
        pq.finished();
        reset(false);
    }
    
    private void setupInputBuffer(SAXSource in) throws InterruptedException {
        Chunk nextIn = pq.nextIn();
        ContentHandler inputBuffer = nextIn.getInput(in);
        XMLFilter suxf = new StateUpdatingXMLFilter(nextIn, in.getXMLReader(), ProcessingState.HAS_INPUT);
        in.setXMLReader(suxf);
        setupParse(inputBuffer);
    }
    
    protected void reset(boolean cancel) {
        try {
            if (outputFuture != null) {
                if (cancel) {
                    outputFuture.cancel(true);
                } else {
                    outputFuture.get();
                }
            }
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        } catch (ExecutionException ex) {
            throw new RuntimeException(ex);
        }
        outputFuture = null;
        consumerThrowable = null;
        producerThrowable = null;
    }

    private XMLReader externalParent;

    @Override
    public void setExecutor(ExecutorService executor) {
        pq.setWorkExecutor(executor == null ? null : new ThrottlingExecutorService(executor));
        super.setExecutor(executor);
    }

    @Override
    public XMLReader getParent() {
        return externalParent;
    }

    @Override
    public void setParent(XMLReader parent) {
        externalParent = parent;
        super.setParent(parent);
    }

    @Override
    public XMLReaderCallback getOutputCallback() {
        return outputCallback;
    }

    @Override
    public void setOutputCallback(XMLReaderCallback callback) {
        this.outputCallback = callback;
    }

    private XMLReaderCallback outputCallback;

    @Override
    public boolean allowOutputCallback() {
        return true;
    }

    private static class StateUpdatingXMLFilter extends VolatileXMLFilterImpl {
        
        private final Chunk outputChunk;
        private final ProcessingState nextState;
        private int level = -1;
        private int recordCount = 0;
        
        private StateUpdatingXMLFilter(Chunk outputChunk, XMLReader parent, ProcessingState nextState) {
            super(parent);
            this.outputChunk = outputChunk;
            this.nextState = nextState;
        }

        @Override
        public void endElement(String uri, String localName, String qName) throws SAXException {
            level--;
            super.endElement(uri, localName, qName);
        }

        @Override
        public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
            level++;
            super.startElement(uri, localName, qName, atts);
            if (level == LevelSplittingXMLFilter.DEFAULT_RECORD_LEVEL) {
                recordCount++;
            }
        }

        @Override
        public void endDocument() throws SAXException {
            super.endDocument(); //To change body of generated methods, choose Tools | Templates.
//            outputChunk.setRecordCount(recordCount);
//            outputChunk.setState(nextState);
        }

        @Override
        public void parse(String systemId) throws SAXException, IOException {
            super.parse(systemId);
            postParse();
        }

        @Override
        public void parse(InputSource input) throws SAXException, IOException {
            super.parse(input);
            postParse();
        }
        
        private void postParse() {
            if (nextState == ProcessingState.READY) {
                outputChunk.getParent().reset();
            } else {
                outputChunk.setRecordCount(recordCount);
                outputChunk.setState(nextState);
            }
        }
        
    }
    
    private static final XMLReader dummyNamespaceAware;
    
    static {
        SAXParserFactory spf = SAXParserFactory.newInstance();
        spf.setNamespaceAware(true);
        try {
            dummyNamespaceAware = spf.newSAXParser().getXMLReader();
        } catch (ParserConfigurationException ex) {
            throw new RuntimeException(ex);
        } catch (SAXException ex) {
            throw new RuntimeException(ex);
        }
    }
    
    private class OutputRunnable implements Runnable {

        private final Thread producer;
        
        private OutputRunnable(Thread producer) {
            this.producer = producer;
        }
        
        private void outputLoop() throws SAXException, IOException {
            Chunk nextOut;
            try {
                while ((nextOut = pq.nextOut()) != null) {
                    VolatileSAXSource outSource = nextOut.getOutput();
                    XMLReader r = new StateUpdatingXMLFilter(nextOut, outSource.getXMLReader(), ProcessingState.READY);
                    outSource.setXMLReader(r);
                    outputCallback.callback(outSource);
                }
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            } catch (SAXException ex) {
                outputCallback.finished(ex);
                throw ex;
            } catch (IOException ex) {
                outputCallback.finished(ex);
                throw ex;
            } catch (Throwable t) {
                outputCallback.finished(t);
                throw new RuntimeException(t);
            }
            outputCallback.finished(null);
        }

        @Override
        public void run() {
            try {
                outputLoop();
            } catch (Throwable t) {
                if (producerThrowable == null) {
                    consumerThrowable = t;
                    producer.interrupt();
                } else {
                    t = producerThrowable;
                    producerThrowable = null;
                    pq.getWorkExecutor().shutdownNow();
                    throw new RuntimeException(t);
                }
            }
        }

    }
    
    private volatile Throwable producerThrowable;
    private volatile Throwable consumerThrowable;
    private Future<?> outputFuture;
    
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
            if (consumerThrowable == null) {
                producerThrowable = t;
                reset(true);
                throw new RuntimeException(t);
            } else {
                t = consumerThrowable;
                consumerThrowable = null;
                pq.getWorkExecutor().shutdownNow();
                throw new RuntimeException(t);
            }
        }
    }

    public void configureOutputTransformer(Transformer out) {
        if (templates != null && out instanceof Controller) {
            Controller controller = (Controller) out;
            try {
                Controller c;
                c = (Controller) templates.newTransformer();
                controller.getExecutable().setCharacterMapIndex(c.getExecutable().getCharacterMapIndex());
                controller.setOutputProperties(c.getOutputProperties());

            } catch (TransformerConfigurationException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    public static final String OUTPUT_TRANSFORMER_PROPERTY_NAME = "http://transform.xml.javax/Transformer#outputTransformer";
    
    @Override
    public void setProperty(String name, Object value) throws SAXNotRecognizedException, SAXNotSupportedException {
        try {
            super.setProperty(name, value);
        } catch (SAXNotRecognizedException ex) {
            if (!OUTPUT_TRANSFORMER_PROPERTY_NAME.equals(name)) {
                throw ex;
            }
        } catch (SAXNotSupportedException ex) {
            if (!OUTPUT_TRANSFORMER_PROPERTY_NAME.equals(name)) {
                throw ex;
            }
        }
        if (OUTPUT_TRANSFORMER_PROPERTY_NAME.equals(name)) {
            configureOutputTransformer((Transformer) value);
        }
    }
    
    @Override
    public boolean getFeature(String name) throws SAXNotRecognizedException, SAXNotSupportedException {
        if (GROUP_BY_SYSTEMID_FEATURE_NAME.equals(name)) {
            if (subdivide) {
                return false;
            } else {
                return super.getFeature(name);
            }
        } else {
            return super.getFeature(name);
        }
    }
    
    

}
