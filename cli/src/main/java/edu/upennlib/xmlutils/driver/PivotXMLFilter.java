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

package edu.upennlib.xmlutils.driver;

import edu.upennlib.paralleltransformer.InputSplitter;
import edu.upennlib.paralleltransformer.JoiningXMLFilter;
import edu.upennlib.paralleltransformer.QueueSourceXMLFilter;
import edu.upenn.library.xmlaminar.parallel.callback.OutputCallback;
import edu.upenn.library.xmlaminar.parallel.callback.XMLReaderCallback;
import edu.upennlib.xmlutils.SAXProperties;
import edu.upennlib.xmlutils.VolatileSAXSource;
import edu.upennlib.xmlutils.dbxml.XMLToPlaintext;
import java.io.IOException;
import java.io.PipedReader;
import java.io.PipedWriter;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Phaser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXNotSupportedException;
import org.xml.sax.XMLFilter;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLFilterImpl;

/**
 *
 * @author magibney
 */
public class PivotXMLFilter<T extends XMLFilter & XMLReader> extends XMLFilterImpl {
    private final JoiningXMLFilter inputSplitterJoiner = new JoiningXMLFilter(false);
    private final int batchSize;
    private final int lookaheadFactor;
    private final XMLFilter highParent;
    private ExecutorService executor;
    private static final Logger logger = LoggerFactory.getLogger(PivotXMLFilter.class);

    public PivotXMLFilter(XMLFilter parent, int batchSize, int lookaheadFactor) {
        this(parent, batchSize, lookaheadFactor, false);
    }
    
    public PivotXMLFilter(XMLFilter parent, int batchSize, int lookaheadFactor, boolean allowFork) {
        this(parent, parent, batchSize, lookaheadFactor, allowFork);
    }
    
    public PivotXMLFilter(XMLFilter highParent, XMLFilter parent, int batchSize, int lookaheadFactor, boolean allowFork) {
        super(parent);
        this.highParent = highParent;
        this.batchSize = batchSize;
        this.lookaheadFactor = lookaheadFactor;
        inputSplitterJoiner.setIteratorWrapper(new InputSplitter(batchSize, lookaheadFactor, allowFork));
        inputSplitterJoiner.setInputType(QueueSourceXMLFilter.InputType.direct);
    }

    @Override
    public void parse(InputSource input) throws SAXException, IOException {
        T parent = (T) getParent();
        inputSplitterJoiner.setParent(parent);
        inputSplitterJoiner.setContentHandler(getContentHandler());
        inputSplitterJoiner.setDTDHandler(getDTDHandler());
        inputSplitterJoiner.setEntityResolver(getEntityResolver());
        inputSplitterJoiner.setErrorHandler(getErrorHandler());
        pivot(highParent.getParent(), input, executor, inputSplitterJoiner, batchSize, lookaheadFactor);
    }

    @Override
    public void setProperty(String name, Object value) throws SAXNotRecognizedException, SAXNotSupportedException {
        boolean valid;
        if (SAXProperties.EXECUTOR_SERVICE_PROPERTY_NAME.equals(name)) {
            if (executor == null) {
                executor = (ExecutorService) value;
            }
            valid = true;
        } else {
            valid = false;
        }
        try {
            super.setProperty(name, value);
        } catch (SAXNotRecognizedException ex) {
            if (!valid) {
                throw ex;
            }
        } catch (SAXNotSupportedException ex) {
            if (!valid) {
                throw ex;
            }
        }
    }
    
    public static InputSource pivot(XMLReader parent, InputSource input, ExecutorService executor, JoiningXMLFilter inputSplitterJoiner,
            int batchSize, int lookaheadFactor) throws SAXException, IOException {
        OutputCallback ocParent;
        if (parent instanceof OutputCallback
                && (ocParent = (OutputCallback) parent).allowOutputCallback()) {
            SplittingCallback sc = new SplittingCallback(inputSplitterJoiner, batchSize, lookaheadFactor, executor);
            ocParent.setOutputCallback(sc);
            /*
            CAN INLINE THIS
             executor.submit(new ParsingRunnable(parent, input));
             sc.awaitFinished();
             */
            try {
                parent.setProperty(SAXProperties.EXECUTOR_SERVICE_PROPERTY_NAME, executor);
            } catch (SAXNotRecognizedException ex) {
                executor.shutdown();
                executor = null;
                logger.trace("ignoring " + ex);
            } catch (SAXNotSupportedException ex) {
                executor.shutdown();
                executor = null;
                logger.trace("ignoring " + ex);
            }
            parent.parse(input);
            sc.awaitFinished();
            return null;
        } else {
            InputSource pivotIn = new InputSource(input.getSystemId());
            pivotIn.setByteStream(input.getByteStream());
            pivotIn.setEncoding(input.getEncoding());
            pivotIn.setPublicId(input.getPublicId());
            PipedReader r = new PipedReader();
            pivotIn.setCharacterStream(r);
            PipedWriter w;
            try {
                w = new PipedWriter(r);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
            XMLToPlaintext xtp = new XMLToPlaintext(w, input, parent);
            executor.submit(xtp);
            inputSplitterJoiner.parse(pivotIn);
            return pivotIn;
        }
    }
    
    private static class SplittingCallback implements XMLReaderCallback {

        private final XMLReader downstream;
        private final ExecutorService executor;
        private final Phaser finished = new Phaser(2);

        public SplittingCallback(XMLReader downstream, int batchSize, int lookaheadFactor, ExecutorService executor) {
            this.downstream = downstream;
            this.executor = executor;
        }
        
        @Override
        public void callback(VolatileSAXSource source) throws SAXException, IOException {
            try {
                XMLReader xmlReader = source.getXMLReader();
                InputSource input = source.getInputSource();
                source.setInputSource(input);
                InputSource pivotIn = new InputSource(input.getSystemId());
                pivotIn.setByteStream(input.getByteStream());
                pivotIn.setEncoding(input.getEncoding());
                pivotIn.setPublicId(input.getPublicId());
                PipedReader r = new PipedReader();
                pivotIn.setCharacterStream(r);
                PipedWriter w;
                try {
                    w = new PipedWriter(r);
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
                XMLToPlaintext xtp = new XMLToPlaintext(w, input, xmlReader);
                executor.submit(xtp);
                downstream.parse(pivotIn);
                finished.arrive();
            } catch (Throwable t) {
                t.printStackTrace(System.err);
                throw new RuntimeException(t);
            }
        }

        public void awaitFinished() {
            finished.arriveAndAwaitAdvance();
        }

        @Override
        public void finished(Throwable t) {
            if (t != null) {
                throw new RuntimeException(t);
            }
        }

    }
    
}
