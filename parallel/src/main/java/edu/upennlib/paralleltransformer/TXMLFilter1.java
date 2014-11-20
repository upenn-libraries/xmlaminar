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

import edu.upennlib.paralleltransformer.callback.OutputCallback;
import edu.upennlib.paralleltransformer.callback.XMLReaderCallback;
import edu.upennlib.xmlutils.UnboundedContentHandlerBuffer;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
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
import org.apache.log4j.Logger;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLFilterImpl;

/**
 *
 * @author Michael Gibney
 */
public class TXMLFilter1 extends QueueSourceXMLFilter implements OutputCallback {

    private static final int DEFAULT_CHUNK_SIZE = 100;
    
    private final ProcessingQueue<Chunk> pq;
    private final Templates templates;
    private int chunkSize = DEFAULT_CHUNK_SIZE;
    private static final Logger LOG = Logger.getLogger(TXMLFilter1.class);
    private ExecutorService executor;

    public TXMLFilter1(Source xslSource) throws TransformerConfigurationException {
        TransformerFactory tf = TransformerFactory.newInstance("net.sf.saxon.TransformerFactoryImpl", null);
        templates = tf.newTemplates(xslSource);
        pq = new ProcessingQueue<Chunk>(100, new Chunk(templates));
    }

    @Override
    protected void initialParse(SAXSource in) {
        outputFuture = executor.submit(new OutputRunnable(Thread.currentThread()));
        pq.reset();
        try {
            setupInputBuffer(in);
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    protected void repeatParse(SAXSource in) {
        try {
            rotateInputBuffer();
            setupInputBuffer(in);
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
    }
    
    @Override
    protected void finished() throws SAXException {
        rotateInputBuffer();
        pq.finished();
    }
    
    private Chunk currentInputBuffer = null;
    
    private void setupInputBuffer(SAXSource in) throws InterruptedException {
        Chunk nextIn = pq.nextIn();
        currentInputBuffer = nextIn;
        UnboundedContentHandlerBuffer inputBuffer = nextIn.getInput();
        inputBuffer.setUnmodifiableParent(in.getXMLReader());
        setupParse(inputBuffer);
    }
    
    private void rotateInputBuffer() {
        Chunk lastIn = currentInputBuffer;
        currentInputBuffer = null;
        lastIn.setState(ProcessingState.HAS_INPUT);
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

    public static void main(String[] args) throws TransformerConfigurationException, SAXException, ParserConfigurationException, FileNotFoundException, TransformerException {
        File in = new File(args[0]);
        File xsl = new File(args[1]);
        File out = new File(args[2]);
        TXMLFilter1 txf = new TXMLFilter1(new StreamSource(xsl));
        SAXParserFactory spf = SAXParserFactory.newInstance();
        spf.setNamespaceAware(true);
        txf.setParent(spf.newSAXParser().getXMLReader());
        TransformerFactory tf = TransformerFactory.newInstance("net.sf.saxon.TransformerFactoryImpl", null);
        Transformer t = tf.newTransformer();
        txf.configureOutputTransformer((Controller) t);
        t.transform(new SAXSource(txf, new InputSource(new FileInputStream(in))), new StreamResult(out));
    }

    private XMLReader externalParent;

    @Override
    public ExecutorService getExecutor() {
        return executor;
    }

    @Override
    public void setExecutor(ExecutorService executor) {
        pq.setWorkExecutor(executor);
        this.executor = executor;
    }

    public int getChunkSize() {
        return chunkSize;
    }

    public void setChunkSize(int chunkSize) {
        if (chunkSize < 1) {
            throw new IllegalArgumentException("chunk size "+chunkSize+" < 1");
        }
        this.chunkSize = chunkSize;
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

    private static class FutureExceptionPropogator implements Runnable {

        private final Thread propogateTo;
        private final Future<?> future;
        
        private FutureExceptionPropogator(Thread propogateTo, Future<?> future) {
            this.propogateTo = propogateTo;
            this.future = future;
        }
        
        @Override
        public void run() {
            try {
                future.get();
            } catch (InterruptedException ex) {
                ex.printStackTrace(System.err);
                future.cancel(true);
            } catch (ExecutionException ex) {
                ex.printStackTrace(System.err);
                propogateTo.interrupt();
            }
        }
        
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

    private static class StateUpdatingXMLFilter extends XMLFilterImpl {
        
        private final Chunk outputChunk;
        
        private StateUpdatingXMLFilter(Chunk outputChunk, XMLReader parent) {
            super(parent);
            this.outputChunk = outputChunk;
        }

        @Override
        public void endDocument() throws SAXException {
            super.endDocument();
            outputChunk.setState(ProcessingState.READY);
        }
        
    }
    
    private class OutputRunnable implements Runnable {

        private final InputSource dummyInputSource = new InputSource();
        private final Thread producer;
        
        private OutputRunnable(Thread producer) {
            this.producer = producer;
        }
        
        private void outputLoop() throws SAXException, IOException {
            while (!pq.isFinished()) {
                try {
                    Chunk nextOut = pq.nextOut();
                    UnboundedContentHandlerBuffer outputBuffer = nextOut.getOutput();
                    outputBuffer.setFlushOnParse(true); //TODO set this behavior by default?
                    XMLReader r = new StateUpdatingXMLFilter(nextOut, outputBuffer);
                    outputCallback.callback(r, dummyInputSource);
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
            }
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
            outputCallback.finished();
        } catch (Throwable t) {
            if (consumerThrowable == null) {
                producerThrowable = t;
                reset(true);
                throw new RuntimeException(t);
            } else {
                t = consumerThrowable;
                consumerThrowable = null;
                throw new RuntimeException(t);
            }
        }
    }

    public void configureOutputTransformer(Controller out) {
        if (templates != null) {
            try {
                Controller c;
                c = (Controller) templates.newTransformer();
                out.getExecutable().setCharacterMapIndex(c.getExecutable().getCharacterMapIndex());
                out.setOutputProperties(c.getOutputProperties());

            } catch (TransformerConfigurationException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

}
