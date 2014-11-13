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

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Scanner;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Pattern;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.XMLFilterImpl;

/**
 *
 * @author Michael Gibney
 */
public abstract class QueueSourceXMLFilter extends XMLFilterImpl {

    public static final InputSource FINISHED = new InputSource();
    public static final String RESET_PROPERTY_NAME = "http://xml.org/sax/features/reset";
    private static final Pattern DEFAULT_DELIMITER_PATTERN = Pattern.compile(System.lineSeparator(), Pattern.LITERAL);
    public static enum InputType { direct, indirect }
    public static InputType DEFAULT_INPUT_TYPE = InputType.indirect;

    private ExecutorService executor;

    private InputType inputType = DEFAULT_INPUT_TYPE;
    private Pattern delimiterPattern = DEFAULT_DELIMITER_PATTERN;

    public static void main(String[] args) throws TransformerConfigurationException, SAXException, ParserConfigurationException, FileNotFoundException, IOException, TransformerException {
        TransformerFactory tf = TransformerFactory.newInstance("net.sf.saxon.TransformerFactoryImpl", null);
        Transformer t = tf.newTransformer();
        File inFile = new File("franklin-small-dump.xml");
        InputSource in = new InputSource(new BufferedInputStream(new FileInputStream(inFile)));
    }

    public void setDelimiterPattern(Pattern p) {
        delimiterPattern = p;
    }
    
    public InputType getInputType() {
        return inputType;
    }
    
    public void setInputType(InputType type) {
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

    /**
     * Implementors should will likely call setupParse(ContentHandler) 
     * or super.setContentHandler(ContentHandler) with an appropriate ContentHandler
     */
    protected abstract void initialParse();
    
    protected abstract void repeatParse();
    
    protected abstract void finished() throws SAXException;
    
    private BlockingQueue<InputSource> parseQueue;

    public void setParseQueue(BlockingQueue<InputSource> queue) {
        parseQueue = queue;
    }
    
    public BlockingQueue<InputSource> getParseQueue() {
        return parseQueue;
    }

    private Future<?> parseQueueSupplier;

    private void initParse() {
        if (parseQueue == null) {
            setParseQueue(new ArrayBlockingQueue<InputSource>(10, false));
        }
        if (executor == null) {
            executor = Executors.newFixedThreadPool(2);
        }
    }
    
    @Override
    public void parse(InputSource input) throws SAXException, IOException {
        initParse();
        Runnable parseQueueRunner = new ParseQueueRunner(input, Thread.currentThread());
        parseQueueSupplier = executor.submit(parseQueueRunner);
        InputSource next;
        try {
            if ((next = parseQueue.take()) != FINISHED) {
                initialParse();
                super.getParent().parse(next);
                while ((next = parseQueue.take()) != FINISHED) {
                    repeatParse();
                    super.getParent().parse(next);
                }
                finished();
            }
        } catch (InterruptedException ex) {
            throw new SAXException(ex);
        }
    }

    @Override
    public void parse(String systemId) throws SAXException, IOException {
        throw new UnsupportedOperationException("not yet implemented");
    }

    protected final boolean setupParse(ContentHandler handler) {
        super.setDTDHandler(this);
        super.setEntityResolver(this);
        super.setErrorHandler(this);
        super.setContentHandler(handler);
        return true;
    }

    private class ParseQueueRunner implements Runnable {

        private final InputSource input;
        private final Thread propogateThrowableTo;

        public ParseQueueRunner(InputSource input, Thread propogateThrowableTo) {
            this.input = input;
            this.propogateThrowableTo = propogateThrowableTo;
        }

        public void handleInput() {
            Reader r;
            if ((r = input.getCharacterStream()) == null) {
                InputStream in;
                if ((in = input.getByteStream()) != null) {
                    try {
                        r = new InputStreamReader(in, input.getEncoding());
                    } catch (UnsupportedEncodingException ex) {
                        throw new RuntimeException(ex);
                    }
                } else {
                    try {
                        r = new BufferedReader(new InputStreamReader(new URI(input.getSystemId()).toURL().openStream(), input.getEncoding()));
                    } catch (IOException ex) {
                        throw new RuntimeException(ex);
                    } catch (URISyntaxException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }
            try {
                Scanner s = new Scanner(r);
                s.useDelimiter(delimiterPattern);
                String next;
                while (s.hasNext()) {
                    next = s.next();
                    parseQueue.add(new InputSource(next));
                }
            } finally {
                parseQueue.add(FINISHED);
                try {
                    r.close();
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            }
        }

        @Override
        public void run() {
            try {
                handleInput();
            } catch (Throwable t) {
                t.printStackTrace(System.err);
                propogateThrowableTo.interrupt();
            }
        }
    }

}
