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

import edu.upennlib.paralleltransformer.callback.IncrementingFileCallback;
import edu.upennlib.paralleltransformer.callback.OutputCallback;
import edu.upennlib.paralleltransformer.callback.StaticFileCallback;
import edu.upennlib.paralleltransformer.callback.XMLReaderCallback;
import edu.upennlib.xmlutils.UnboundedContentHandlerBuffer;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
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
import org.apache.log4j.Logger;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLFilter;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLFilterImpl;

/**
 *
 * @author Michael Gibney
 */
public class TXMLFilter extends QueueSourceXMLFilter implements OutputCallback {

    private final ProcessingQueue<Chunk> pq;
    private final Templates templates;
    private static final Logger LOG = Logger.getLogger(TXMLFilter.class);

    public TXMLFilter(Source xslSource) throws TransformerConfigurationException {
        TransformerFactory tf = TransformerFactory.newInstance("net.sf.saxon.TransformerFactoryImpl", null);
        templates = tf.newTemplates(xslSource);
        pq = new ProcessingQueue<Chunk>(100, new Chunk(templates));
    }

    @Override
    protected void initialParse(SAXSource in) {
        outputFuture = getExecutor().submit(new OutputRunnable(Thread.currentThread()));
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
            setupInputBuffer(in);
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
    }
    
    @Override
    protected void finished() throws SAXException {
        pq.finished();
    }
    
    private void setupInputBuffer(SAXSource in) throws InterruptedException {
        Chunk nextIn = pq.nextIn();
        UnboundedContentHandlerBuffer inputBuffer = nextIn.getInput();
        XMLFilter suxf = new StateUpdatingXMLFilter(nextIn, in.getXMLReader(), ProcessingState.HAS_INPUT);
        inputBuffer.setUnmodifiableParent(suxf);
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
        pq.setWorkExecutor(executor);
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

    private static class StateUpdatingXMLFilter extends XMLFilterImpl {
        
        private final Chunk outputChunk;
        private final ProcessingState nextState;
        
        private StateUpdatingXMLFilter(Chunk outputChunk, XMLReader parent, ProcessingState nextState) {
            super(parent);
            this.outputChunk = outputChunk;
            this.nextState = nextState;
        }

        @Override
        public void endDocument() throws SAXException {
            super.endDocument();
            outputChunk.setState(nextState);
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
                    outputBuffer.setUnmodifiableParent(dummyNamespaceAware);
                    outputBuffer.setFlushOnParse(true); //TODO set this behavior by default?
                    XMLReader r = new StateUpdatingXMLFilter(nextOut, outputBuffer, ProcessingState.READY);
                    outputCallback.callback(r, dummyInputSource);
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
            }
            outputCallback.finished();
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
            outputFuture.get(); // wait for output to complete before returning
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

}
