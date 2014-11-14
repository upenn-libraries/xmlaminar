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

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Pattern;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
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
public abstract class QueueSourceXMLFilter extends XMLFilterImpl {

    public static final InputSource FINISHED = new InputSource();
    public static final String RESET_PROPERTY_NAME = "http://xml.org/sax/features/reset";
    private static final Pattern DEFAULT_DELIMITER_PATTERN = Pattern.compile(System.lineSeparator(), Pattern.LITERAL);
    private static final Logger LOG = LoggerFactory.getLogger(QueueSourceXMLFilter.class);
    public static enum InputType { direct, indirect }
    public static InputType DEFAULT_INPUT_TYPE = InputType.indirect;

    private ExecutorService executor;

    private InputType inputType = DEFAULT_INPUT_TYPE;
    private Pattern delimiterPattern = DEFAULT_DELIMITER_PATTERN;

    public static void main(String[] args) throws TransformerConfigurationException, SAXException, ParserConfigurationException, FileNotFoundException, IOException, TransformerException {
        JoiningXMLFilter.main(args);
    }

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
     * Implementors should will likely call setupParse(ContentHandler) 
     * or super.setContentHandler(ContentHandler) with an appropriate ContentHandler
     */
    protected abstract void initialParse(InputSource in);
    
    protected abstract void repeatParse(InputSource in);
    
    protected abstract void finished() throws SAXException;
    
    private BlockingQueue<InputSource> parseQueue;

    public void setParseQueue(BlockingQueue<InputSource> queue) {
        parseQueue = queue;
    }
    
    public BlockingQueue<InputSource> getParseQueue() {
        return parseQueue;
    }

    private Future<?> parseQueueSupplier;

    private void initProducer(InputSource input) {
        if (parseQueue == null) {
            setParseQueue(new ArrayBlockingQueue<InputSource>(10, false));
        }
        if (executor == null) {
            executor = Executors.newFixedThreadPool(1);
        }
        Runnable parseQueueRunner = new ParseQueueSupplier(input, Thread.currentThread());
        parseQueueSupplier = executor.submit(parseQueueRunner);
        InputSource next;
        try {
            if ((next = parseQueue.take()) != FINISHED) {
                initialParse(next);
                super.parse(next);
                while ((next = parseQueue.take()) != FINISHED) {
                    repeatParse(next);
                    super.parse(next);
                }
                finished();
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
    
    private void setupParseLocal() {
        XMLReader parent = super.getParent();
        parent.setDTDHandler(this);
        parent.setEntityResolver(this);
        parent.setErrorHandler(this);
        parent.setContentHandler(this);
    }
    
    @Override
    public void parse(InputSource input) throws SAXException, IOException {
        setupParseLocal();
        switch (inputType) {
            case direct:
                initialParse(input);
                super.parse(input);
                finished();
                break;
            case indirect:
                initProducer(input);
                break;
            default:
                throw new IllegalStateException("input type should not be null");
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

    private class ParseQueueSupplier implements Runnable {

        private final InputSource input;
        private final Thread consumer;

        public ParseQueueSupplier(InputSource input, Thread consumerThread) {
            this.input = input;
            this.consumer = consumerThread;
        }

        public void handleInput() throws IOException, InterruptedException, URISyntaxException {
            Reader r;
            if ((r = input.getCharacterStream()) == null) {
                InputStream in;
                String charsetName = input.getEncoding();
                Charset encoding = (charsetName == null ? Charset.defaultCharset() : Charset.forName(charsetName));
                if ((in = input.getByteStream()) != null) {
                    r = new InputStreamReader(in, encoding);
                } else {
                    System.out.println(input.getSystemId());
                    r = new BufferedReader(new InputStreamReader(new URI(input.getSystemId()).toURL().openStream(), encoding));
                }
            }
            boolean interrupted = false;
            try {
                Scanner s = new Scanner(r);
                s.useDelimiter(delimiterPattern);
                String next;
                while (s.hasNext()) {
                    next = s.next();
                    parseQueue.put(new InputSource(next));
                }
            } catch (InterruptedException ex) {
                interrupted = true;
                throw ex;
            } finally {
                r.close();
                if (!interrupted) {
                    parseQueue.put(FINISHED);
                }
            }
        }

        @Override
        public void run() {
            try {
                handleInput();
            } catch (Throwable t) {
                if (consumerThrowable == null) {
                    producerThrowable = t;
                    consumer.interrupt();
                } else {
                    t = consumerThrowable;
                    consumerThrowable = null;
                    throw new RuntimeException(t);
                }
            }
        }
    }

}
