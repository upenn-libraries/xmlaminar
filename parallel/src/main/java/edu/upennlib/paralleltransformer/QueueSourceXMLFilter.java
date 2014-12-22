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
import edu.upennlib.paralleltransformer.callback.QueueDestCallback;
import edu.upennlib.xmlutils.VolatileXMLFilterImpl;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.regex.Pattern;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.sax.SAXSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLFilterImpl;

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
    
    public SAXSource getFinishedSAXSource(Throwable t) {
        return FINISHED.checkout(t);
    }
    
    private static class ErrorSAXSource extends SAXSource {
        
        private final AtomicBoolean checkedOutError = new AtomicBoolean(false);
        private Throwable t;
        
        private SAXSource checkout(Throwable t) {
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
    protected abstract void initialParse(SAXSource in);
    
    protected abstract void repeatParse(SAXSource in);
    
    protected abstract void finished() throws SAXException;
    
    private BlockingQueue<SAXSource> parseQueue;
    private Future<?> parseQueueSupplier;

    public void setParseQueue(BlockingQueue<SAXSource> queue) {
        parseQueue = queue;
    }
    
    public BlockingQueue<SAXSource> getParseQueue() {
        return parseQueue;
    }
    
    public void setParseQueueSupplier(Future<?> parseQueueSupplier) {
        this.parseQueueSupplier = parseQueueSupplier;
    }
    
    public Future<?> getParseQueueSupplier() {
        return parseQueueSupplier;
    }

    private void initProducer(InputSource input) {
        if (parseQueue == null) {
            parseQueue = new ArrayBlockingQueue<SAXSource>(10, false);
        }
        if (parseQueueSupplier == null) {
            Runnable parseQueueRunner = new ParseQueueSupplier(input, Thread.currentThread());
            parseQueueSupplier = executor.submit(parseQueueRunner);
        }
        SAXSource next;
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
                if (FINISHED.t != null) {
                    throw new RuntimeException(FINISHED.t);
                }
                finished();
            }
            if (FINISHED.t != null) {
                throw new RuntimeException(FINISHED.t);
            }
        } catch (Throwable ex) {
            if (producerThrowable == null) {
                consumerThrowable = ex;
                parseQueueSupplier.cancel(true);
                throw new RuntimeException(ex);
            } else {
                ex = producerThrowable;
                producerThrowable = null;
                throw new RuntimeException(ex);
            }
        }
    }
    
    private void initProducerIterator(InputSource input) {
        try {
            SAXSource next;
            Iterator<SAXSource> sourceIter = new IndirectSourceSupplier(input);
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
    
    private void setupParseLocal() throws SAXException {
        XMLReader localParent = getParent();
        if (localParent instanceof OutputCallback) {
            setInputType(InputType.queue);
            ((OutputCallback)localParent).setOutputCallback(new QueueDestCallback(this));
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
                qsxfp.setExecutor(executor);
            }
        }
        XMLReader parent = super.getParent();
        System.out.println("parent of "+this+" is "+parent);
        parent.setDTDHandler(this);
        parent.setEntityResolver(this);
        parent.setErrorHandler(this);
        parent.setContentHandler(this);
    }
    
    @Override
    public void parse(InputSource input) throws SAXException, IOException {
        try {
            setupParseLocal();
            System.out.println(inputType);
            switch (inputType) {
                case direct:
                    SAXSource source = new SAXSource(super.getParent(), input);
                    initialParse(source);
                    XMLReader xmlReader = source.getXMLReader();
                    xmlReader.setContentHandler(this);
                    xmlReader.parse(source.getInputSource());
                    finished();
                    break;
                case indirect:
                    initProducerIterator(input);
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

    private class IndirectSourceSupplier implements Iterator<SAXSource> {

        private final InputSource input;
        private final Scanner s;

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
            if (s.hasNext()) {
                return true;
            } else {
                s.close();
                return false;
            }
        }

        @Override
        public SAXSource next() {
            String next = s.next();
            InputSource nextIn = new InputSource(next);
            XMLReader xr = QueueSourceXMLFilter.super.getParent(); // defaults to parent
            return new SAXSource(xr, nextIn);
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Not supported.");
        }

    }
    
    /**
     * Exists strictly to hand off control to upstream parsing that will 
     * supply input to this XMLFilter via queue, and propagate exceptions 
     * back to the calling thread.
     */
    private class ParseQueueSupplier implements Runnable {

        private final InputSource input;
        private final Thread consumer;

        public ParseQueueSupplier(InputSource input, Thread consumerThread) {
            this.input = input;
            this.consumer = consumerThread;
        }

        @Override
        public void run() {
            try {
                QueueSourceXMLFilter.super.parse(input);
            } catch (Throwable t) {
                t.printStackTrace(System.err);
                if (consumerThrowable == null) {
                    producerThrowable = t;
                    consumer.interrupt();
                    throw new RuntimeException(t);
                } else {
                    t = consumerThrowable;
                    consumerThrowable = null;
                    throw new RuntimeException(t);
                }
            }
        }
    }

}
