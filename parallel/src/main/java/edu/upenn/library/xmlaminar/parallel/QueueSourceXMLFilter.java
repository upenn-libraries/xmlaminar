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

package edu.upenn.library.xmlaminar.parallel;

import edu.upenn.library.xmlaminar.parallel.callback.OutputCallback;
import edu.upenn.library.xmlaminar.parallel.callback.QueueDestCallback;
import edu.upenn.library.xmlaminar.VolatileSAXSource;
import edu.upenn.library.xmlaminar.VolatileXMLFilterImpl;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.MalformedURLException;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.regex.Pattern;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

/**
 *
 * @author Michael Gibney
 */
public abstract class QueueSourceXMLFilter extends VolatileXMLFilterImpl {

    private final ErrorSAXSource FINISHED = new ErrorSAXSource();
    public static final String RESET_PROPERTY_NAME = "http://xml.org/sax/features/reset";
    private static final Pattern DEFAULT_DELIMITER_PATTERN = Pattern.compile(System.lineSeparator(), Pattern.LITERAL);
    private static final Logger LOG = LoggerFactory.getLogger(QueueSourceXMLFilter.class);
    public static enum InputType { direct, indirect, queue }
    public static InputType DEFAULT_INPUT_TYPE = InputType.direct;
    
    public VolatileSAXSource getFinishedSAXSource(Throwable t) {
        return FINISHED.checkout(t);
    }
    
    private static class ErrorSAXSource extends VolatileSAXSource {
        
        private final AtomicBoolean checkedOutError = new AtomicBoolean(false);
        private volatile Throwable t;
        
        private void reset() {
            checkedOutError.set(false);
            t = null;
        }
        
        private VolatileSAXSource checkout(Throwable t) {
            if (t == null) {
                return this;
            } else if (!checkedOutError.compareAndSet(false, true)) {
                throw new IllegalStateException("already checked out");
            } else {
                this.t = t;
                return this;
            }
        }
        
    }
    
    private static final SAXParserFactory spf;
    
    static {
        spf = SAXParserFactory.newInstance();
        spf.setNamespaceAware(true);
    }
    
    public QueueSourceXMLFilter() {
        try {
            setParent(spf.newSAXParser().getXMLReader());
        } catch (ParserConfigurationException ex) {
            throw new RuntimeException(ex);
        } catch (SAXException ex) {
            throw new RuntimeException(ex);
        }
    }
    
    private ExecutorService executor;

    private InputType inputType = DEFAULT_INPUT_TYPE;
    private Pattern delimiterPattern = DEFAULT_DELIMITER_PATTERN;

    public void setDelimiterPattern(Pattern p) {
        delimiterPattern = p;
    }
    
    public InputType getInputType() {
        return inputType;
    }
    
    public void setInputType(InputType type) {
        if (type == null) {
            throw new IllegalArgumentException("inputType must not be null");
        }
        inputType = type;
    }

    public Pattern getDelimiterPattern() {
        return delimiterPattern;
    }

    public ExecutorService getExecutor() {
        return executor;
    }

    public void setExecutor(ExecutorService executor) {
        this.executor = executor;
    }
    
    public void shutdown() {
        if (executor != null) {
            executor.shutdown();
        }
    }
    
    public List<Runnable> shutdownNow() {
        if (executor == null) {
            return Collections.EMPTY_LIST;
        } else {
            return executor.shutdownNow();
        }
    }

    /**
     * Implementors will likely call setupParse(ContentHandler) 
     * or super.setContentHandler(ContentHandler) with an appropriate ContentHandler
     */
    protected abstract void initialParse(VolatileSAXSource in);
    
    protected abstract void repeatParse(VolatileSAXSource in);
    
    protected abstract void finished() throws SAXException;
    
    private BlockingQueue<VolatileSAXSource> parseQueue;
    private Future<?> parseQueueSupplier;

    public void setParseQueue(BlockingQueue<VolatileSAXSource> queue) {
        parseQueue = queue;
    }
    
    public BlockingQueue<VolatileSAXSource> getParseQueue() {
        return parseQueue;
    }
    
    public void setParseQueueSupplier(Future<?> parseQueueSupplier) {
        this.parseQueueSupplier = parseQueueSupplier;
    }
    
    public Future<?> getParseQueueSupplier() {
        return parseQueueSupplier;
    }

    public static interface QueueSource<T> {
        BlockingQueue<T> newQueue();
    } 
    
    private void initProducer(InputSource input) {
        if (parseQueue == null) {
            XMLReader parent = getParent();
            if (parent instanceof QueueSource) {
                parseQueue = ((QueueSource) parent).newQueue();
            } else {
                parseQueue = new ArrayBlockingQueue<VolatileSAXSource>(10, false);
            }
        } else {
            parseQueue.clear();
        }
        boolean setPQS = false;
        parseThread = Thread.currentThread();
        if (parseQueueSupplier == null) {
            setPQS = true;
            Runnable parseQueueRunner = new ParseQueueSupplier(input);
            parseQueueSupplier = executor.submit(parseQueueRunner);
        }
        VolatileSAXSource next;
        try {
            if ((next = parseQueue.take()) != FINISHED) {
                initialParse(next);
                XMLReader xmlReader = next.getXMLReader();
                xmlReader.setContentHandler(this);
                xmlReader.parse(next.getInputSource());
                while ((next = parseQueue.take()) != FINISHED) {
                    repeatParse(next);
                    xmlReader = next.getXMLReader();
                    xmlReader.setContentHandler(this);
                    xmlReader.parse(next.getInputSource());
                }
            }
            if (FINISHED.t == null) {
                finished();
            } else {
                throw new UpstreamException(FINISHED.t);
            }
        } catch (UpstreamException ex) {
            throw ex;
        } catch (Throwable t) {
            throw handleDownstreamException(t);
        } finally {
            if (setPQS) {
                parseQueueSupplier = null;
            }
            parseThread = null;
        }
    }

    private RuntimeException handleDownstreamException(Throwable t) {
        if (parseThread != null) {
            parseThread = null;
            parseQueueSupplier.cancel(true);
        }
        VolatileSAXSource vss;
        try {
            if ((vss = parseQueue.take()) != FINISHED) {
                LOG.error("parseQueueSupplier must send FINISHED signal; got "+vss);
            }
        } catch (InterruptedException ex) {
            LOG.warn("interrupted while handling downstream exception");
        }
        return new RuntimeException(t);
    }
    
    private static class UpstreamException extends RuntimeException {

        public UpstreamException() {
        }

        public UpstreamException(String message) {
            super(message);
        }

        public UpstreamException(String message, Throwable cause) {
            super(message, cause);
        }

        public UpstreamException(Throwable cause) {
            super(cause);
        }

        public UpstreamException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
            super(message, cause, enableSuppression, writableStackTrace);
        }
        
    }
    
    public static interface IteratorWrapper<T> {
        Iterator<T> wrapIterator(Iterator<T> base);
    }
    
    private IteratorWrapper<VolatileSAXSource> iteratorWrapper;
    
    public IteratorWrapper<VolatileSAXSource> getIteratorWrapper() {
        return iteratorWrapper;
    }
    
    public void setIteratorWrapper(IteratorWrapper<VolatileSAXSource> iteratorWrapper) {
        this.iteratorWrapper = iteratorWrapper;
    }
    
    private Iterator<VolatileSAXSource> initIterator(InputSource input, InputType induced) {
        if (iteratorWrapper == null) {
            return new IndirectSourceSupplier(input);
        } else {
            Iterator<VolatileSAXSource> base;
            switch (inputType) {
                case indirect:
                    base = new IndirectSourceSupplier(input);
                    break;
                case direct:
                    base = Collections.singletonList(new VolatileSAXSource(super.getParent(), input)).iterator();
                    break;
                default:
                    throw new AssertionError("should never reach here");
            }
            return iteratorWrapper.wrapIterator(base);
        }
    }
    
    private void initProducerIterator(InputSource input, InputType induced) {
        try {
            VolatileSAXSource next;
            Iterator<VolatileSAXSource> sourceIter = initIterator(input, induced);
            if (sourceIter.hasNext()) {
                next = sourceIter.next();
                initialParse(next);
                XMLReader xmlReader = next.getXMLReader();
                xmlReader.setContentHandler(this);
                xmlReader.parse(next.getInputSource());
                while (sourceIter.hasNext()) {
                    next = sourceIter.next();
                    repeatParse(next);
                    xmlReader = next.getXMLReader();
                    xmlReader.setContentHandler(this);
                    xmlReader.parse(next.getInputSource());
                }
            }
            finished();
        } catch (SAXException ex) {
            throw new RuntimeException(ex);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }
    
    private boolean createdExecutor = false;
    private boolean autoSetParentExecutor = false;
    
    private void setupParseLocal() throws SAXException {
        FINISHED.reset();
        producerThrowable = null;
        consumerThrowable = null;
        XMLReader localParent = getParent();
        if (localParent instanceof OutputCallback) {
            OutputCallback oc = (OutputCallback) localParent;
            if (oc.allowOutputCallback()) {
                setInputType(InputType.queue);
                oc.setOutputCallback(new QueueDestCallback(this));
            }
        }
        if (parseQueueSupplier == null) {
            if (executor == null) {
                createdExecutor = true;
                setExecutor(Executors.newCachedThreadPool());
            }
        }
        if (localParent instanceof QueueSourceXMLFilter && executor != null) {
            QueueSourceXMLFilter qsxfp = (QueueSourceXMLFilter)localParent;
            if (qsxfp.getExecutor() == null) {
                autoSetParentExecutor = true;
                qsxfp.setExecutor(executor);
            }
        }
        XMLReader parent = super.getParent();
        parent.setDTDHandler(this);
        parent.setEntityResolver(this);
        parent.setErrorHandler(this);
        parent.setContentHandler(this);
    }
    
    @Override
    public void parse(InputSource input) throws SAXException, IOException {
        try {
            setupParseLocal();
            InputType induced;
            if (iteratorWrapper == null) {
                induced = inputType;
            } else {
                switch (inputType) {
                    case direct:
                        induced = InputType.indirect;
                        break;
                    case indirect:
                        induced = inputType;
                        break;
                    default:
                        throw new IllegalStateException("cannot wrap iterator for inputType="+inputType);
                }
            }
            switch (induced) {
                case direct:
                    VolatileSAXSource source = new VolatileSAXSource(super.getParent(), input);
                    initialParse(source);
                    XMLReader xmlReader = source.getXMLReader();
                    xmlReader.setContentHandler(this);
                    xmlReader.parse(source.getInputSource());
                    finished();
                    break;
                case indirect:
                    initProducerIterator(input, induced);
                    break;
                case queue:
                    initProducer(input);
                    break;
                default:
                    throw new IllegalStateException("input type should not be null");
            }
        } finally {
            if (createdExecutor) {
                executor.shutdown();
                executor = null;
                createdExecutor = false;
            }
            if (autoSetParentExecutor) {
                ((QueueSourceXMLFilter)getParent()).setExecutor(null);
                autoSetParentExecutor = false;
            }
        }
    }

    private volatile Throwable producerThrowable;
    private volatile Throwable consumerThrowable;
    
    @Override
    public void parse(String systemId) throws SAXException, IOException {
        throw new UnsupportedOperationException("not yet implemented");
    }

    /**
     * Sets the content handler on the underlying XMLFilterImpl implementation; events
     * passed to <code>super</code> method implementations will be (initially)
     * handled by this handler.
     * @param handler
     * @return 
     */
    protected final boolean setupParse(ContentHandler handler) {
        super.setContentHandler(handler);
        return true;
    }

    private class IndirectSourceSupplier implements Iterator<VolatileSAXSource> {

        private final InputSource input;
        private final Scanner s;
        private boolean scannerClosed = false;

        public IndirectSourceSupplier(InputSource input) {
            this.input = input;
            s = getScanner();
        }
        
        private Scanner getScanner() {
            Reader r;
            if ((r = input.getCharacterStream()) == null) {
                InputStream in;
                String charsetName = input.getEncoding();
                Charset encoding = (charsetName == null ? Charset.defaultCharset() : Charset.forName(charsetName));
                if ((in = input.getByteStream()) != null) {
                    r = new InputStreamReader(in, encoding);
                } else {
                    try {
                        File inputPath = new File(input.getSystemId()).getAbsoluteFile();
                        r = new BufferedReader(new InputStreamReader(inputPath.toURI().toURL().openStream(), encoding));
                    } catch (MalformedURLException ex) {
                        throw new RuntimeException(ex);
                    } catch (IOException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }
            Scanner s = new Scanner(r);
            s.useDelimiter(delimiterPattern);
            return s;
        }

        @Override
        public boolean hasNext() {
            if (scannerClosed) {
                return false;
            } else if (s.hasNext()) {
                return true;
            } else {
                scannerClosed = true;
                s.close();
                return false;
            }
        }

        @Override
        public VolatileSAXSource next() {
            String next = s.next();
            InputSource nextIn = new InputSource(next);
            XMLReader xr = QueueSourceXMLFilter.super.getParent(); // defaults to parent
            return new VolatileSAXSource(xr, nextIn);
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Not supported.");
        }

    }
    
    private volatile Thread parseThread = null;
    
    private void handleUpstreamThrowable(Throwable t) {
        parseQueue.clear();
        if (parseThread != null) {
            Thread pt = parseThread;
            parseThread = null;
            pt.interrupt();
        }
        try {
            parseQueue.put(getFinishedSAXSource(t));
        } catch (InterruptedException ex) {
            LOG.warn("interrupted while handling upstream throwable", t);
            throw new RuntimeException(ex);
        }
    }

    /**
     * Exists strictly to hand off control to upstream parsing that will 
     * supply input to this XMLFilter via queue, and propagate exceptions 
     * back to the calling thread.
     */
    private class ParseQueueSupplier implements Runnable {

        private final InputSource input;

        public ParseQueueSupplier(InputSource input) {
            this.input = input;
        }

        @Override
        public void run() {
            try {
                QueueSourceXMLFilter.super.parse(input);
            } catch (Throwable t) {
                handleUpstreamThrowable(t);
                throw new RuntimeException(t);
            }
        }
    }

}
