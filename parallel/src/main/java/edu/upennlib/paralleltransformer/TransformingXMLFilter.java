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

import edu.upennlib.xmlutils.JoiningXMLFilter;
import edu.upennlib.xmlutils.LoggingErrorListener;
import edu.upennlib.xmlutils.SimpleLocalAbsoluteSAXXPath;
import edu.upennlib.xmlutils.SplittingXMLFilter;
import edu.upennlib.xmlutils.UnboundedContentHandlerBuffer;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.StringWriter;
import java.util.EnumMap;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Source;
import javax.xml.transform.Templates;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.URIResolver;
import javax.xml.transform.sax.SAXResult;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.sax.SAXTransformerFactory;
import javax.xml.transform.sax.TransformerHandler;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import net.sf.saxon.Controller;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXNotSupportedException;
import org.xml.sax.XMLReader;
import org.xml.sax.ContentHandler;
import org.xml.sax.ext.LexicalHandler;
import org.xml.sax.helpers.XMLFilterImpl;

/**
 *
 * @author michael
 */
public class TransformingXMLFilter extends JoiningXMLFilter {

    private SplittingXMLFilter splitter;
    private SplittingXMLFilter privateSplitter;
    private File stylesheet;
    private InputStream stylesheetStream;
    private Templates stylesheetTemplates;
    public static final String TRANSFORMER_FACTORY_CLASS_NAME = "net.sf.saxon.TransformerFactoryImpl";
    private static final SAXTransformerFactory stf = (SAXTransformerFactory) TransformerFactory.newInstance(TRANSFORMER_FACTORY_CLASS_NAME, null);
    private static final String USAGE = "USAGE: command inputFile stylesheet outputFile";
    private static final Logger logger = Logger.getLogger(TransformingXMLFilter.class);

    private static void printUsage(String errorMessage, int exitCode) {
        System.err.println(errorMessage);
        System.err.println(USAGE);
        System.exit(exitCode);
    }

    public static void main(String[] args) throws SAXException, ParserConfigurationException, TransformerConfigurationException, TransformerException, IOException {
        BasicConfigurator.configure();
        if (args.length != 3) {
            printUsage("wrong number of arguments: "+args.length, -1);
        }

        File inputFile = new File(args[0]);
        File stylesheet = new File(args[1]);
        File outputFile = new File(args[2]);

        TransformingXMLFilter txf = new TransformingXMLFilter();
        try {
            txf.setStylesheet(stylesheet);
        } catch (FileNotFoundException ex) {
            printUsage(ex.getMessage(), -1);
        }

        InputSource inputSource = null;
        try {
            inputSource = new InputSource(new BufferedInputStream(new FileInputStream(inputFile)));
        } catch (FileNotFoundException ex) {
            printUsage(ex.getMessage(), -1);
        }

        BufferedOutputStream bos = null;
        try {
            bos = new BufferedOutputStream(new FileOutputStream(outputFile));
        } catch (FileNotFoundException ex) {
            printUsage(ex.getMessage(), -1);
        }

        Transformer t = stf.newTransformer();
        txf.configureOutputTransformer((Controller) t);

        long start = System.currentTimeMillis();
        t.transform(new SAXSource(txf, inputSource), new StreamResult(bos));
        bos.close();
        System.out.println("transformation complete; duration: " + (System.currentTimeMillis() - start)+ " ms");
    }

    public TransformingXMLFilter() throws ParserConfigurationException, SAXException {
        privateSplitter = new SplittingXMLFilter();
        super.setParent(privateSplitter);
        setUpstreamSplittingFilter(privateSplitter);

        SAXParserFactory spf = SAXParserFactory.newInstance();
        spf.setNamespaceAware(true);
        XMLReader reader = spf.newSAXParser().getXMLReader();
        privateSplitter.setParent(reader);
    }

    @Override
    public void setParent(XMLReader parent) {
        super.setParent(parent);
        privateSplitter = null;
    }

    public void setStreamingParent(XMLReader parent) {
        if (privateSplitter == null) {
            privateSplitter = new SplittingXMLFilter();
            super.setParent(privateSplitter);
            setUpstreamSplittingFilter(privateSplitter);
        }
        privateSplitter.setParent(parent);
    }

    public XMLReader getStreamingParent() {
        if (privateSplitter == null) {
            return null;
        } else {
            return privateSplitter.getParent();
        }
    }

    public void setStylesheetStream(InputStream stylesheetStream) throws TransformerConfigurationException, FileNotFoundException {
        if (stylesheetStream == null) {
            stylesheetTemplates = null;
        } else {
            stylesheetTemplates = stf.newTemplates(new StreamSource(stylesheetStream));
        }
        this.stylesheetStream = stylesheetStream;
    }

    public InputStream getStylesheetStream() {
        return stylesheetStream;
    }

    public void setStylesheet(File stylesheet) throws TransformerConfigurationException, FileNotFoundException {
        if (stylesheet == null) {
            stylesheetTemplates = null;
        } else {
            stylesheetTemplates = stf.newTemplates(new StreamSource(stylesheet));
        }
        this.stylesheet = stylesheet;
    }

    public File getStylesheet() {
        return stylesheet;
    }

    public void configureOutputTransformer(Controller out) {
        if (stylesheetTemplates != null) {
            try {
                Controller c;
                c = (Controller) stylesheetTemplates.newTransformer();
                out.getExecutable().setCharacterMapIndex(c.getExecutable().getCharacterMapIndex());
                out.setOutputProperties(c.getOutputProperties());

            } catch (TransformerConfigurationException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    public final void setUpstreamSplittingFilter(SplittingXMLFilter splitter) {
        this.splitter = splitter;
    }

    @Override
    public void parse(InputSource input) throws SAXException, IOException {
        parse(input, null);
    }

    @Override
    public void parse(String systemId) throws SAXException, IOException {
        parse(null, systemId);
    }

    private void parse(InputSource input, String systemId) throws SAXException, IOException {
        if (splitter != null) {
            int BUFFER_SIZE = 10;
            XMLReader parent = super.getParent();
            parent.setDTDHandler(this);
            parent.setErrorHandler(this);
            parent.setEntityResolver(this);
            TransformerOutputBuffer tob;
            try {
                tob = new TransformerOutputBuffer(BUFFER_SIZE, this, stylesheetTemplates);
            } catch (TransformerConfigurationException ex) {
                throw new RuntimeException(ex);
            }
            ExecutorService executor = Executors.newCachedThreadPool();
            Thread outputThread = new Thread(tob, "outputThread");
            outputThread.start();
            if (input != null) {
                do {
                    TransformerOutputBuffer.TransformerRunner tr = tob.checkOut();
                    parent.setContentHandler(tr.getInputBuffer());
                    parent.parse(input);
                    executor.execute(tr);
                } while (splitter.hasMoreOutput(input));
            } else {
                do {
                    TransformerOutputBuffer.TransformerRunner tr = tob.checkOut();
                    parent.setContentHandler(tr.getInputBuffer());
                    parent.parse(systemId);
                    executor.execute(tr);
                } while (splitter.hasMoreOutput(systemId));
            }
            tob.notifyFinished();
            try {
                outputThread.join();
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            }
            executor.shutdown();
            finished();
        }
    }

    private static class TransformerOutputBuffer implements Runnable {

        public static enum State { EMPTY, CHECKED_OUT, WRITABLE, EOF };
        private final int size;
        private final TransformerRunner[] trs;
        private int head = 0;
        private int tail = 0;
        private final Lock inputLock = new ReentrantLock();
        private final Condition spaceAvailable = inputLock.newCondition();
        private long totalChunks = 0;
        private long blockInput = 0;
        private AtomicLong blockOutput = new AtomicLong(0);
        private boolean notifyInput = false;
        private final ContentHandler output;
        private final LexicalHandler lexOutput;

        public TransformerOutputBuffer(int size, XMLFilterImpl transformerFilter, Templates temp) throws TransformerConfigurationException {
            this.size = size;
            this.output = transformerFilter;
            if (transformerFilter instanceof LexicalHandler) {
                lexOutput = (LexicalHandler) transformerFilter;
            } else {
                lexOutput = null;
            }
            trs = new TransformerRunner[size];
            for (int i = 0; i < size; i++) {
                UnboundedContentHandlerBuffer input = new UnboundedContentHandlerBuffer();
                input.setUnmodifiableParent(transformerFilter);
                input.setParentModifiable(false);
                trs[i] = new TransformerRunner(temp.newTransformer(), input, new UnboundedContentHandlerBuffer(), blockOutput);
            }
        }

        public TransformerRunner checkOut() {
            TransformerRunner trTail = trs[tail];
            if (trTail.state != State.EMPTY) {
                notifyInput = true;
                try {
                    inputLock.lock();
                    while (trTail.state != State.EMPTY) {
                        try {
                            blockInput++;
                            spaceAvailable.await();
                        } catch (InterruptedException ex) {
                            throw new RuntimeException(ex);
                        }
                    }
                    notifyInput = false;
                } finally {
                    inputLock.unlock();
                }
            }
            tail = (tail + 1) % size;
            trTail.checkOut();
            totalChunks++;
            return trTail;
        }

        public void notifyFinished() {
            TransformerRunner trTail = trs[tail];
            if (trTail.state != State.EMPTY) {
                notifyInput = true;
                try {
                    inputLock.lock();
                    while (trTail.state != State.EMPTY) {
                        try {
                            spaceAvailable.await();
                        } catch (InterruptedException ex) {
                            throw new RuntimeException(ex);
                        }
                    }
                    notifyInput = false;
                } finally {
                    inputLock.unlock();
                }
            }
            trTail.notifyFinished();
        }

        @Override
        public void run() {
            try {
                while (true) {
                    try {
                        trs[head].writeOutputTo(output, lexOutput);
                    } catch (SAXException ex) {
                        throw new RuntimeException(ex);
                    }
                    if (notifyInput) {
                        try {
                            inputLock.lock();
                            spaceAvailable.signal();
                        } finally {
                            inputLock.unlock();
                        }
                    }
                    head = (head + 1) % size;
                }
            } catch (EOFException ex) {
                // Ok thread exit.
            }
            System.out.println("transformer i/o chunks=" + totalChunks + "; blockInput=" + blockInput + "; blockOutput=" + blockOutput.get());
        }

        public static class TransformerRunner implements Runnable {

            private final Lock outputLock = new ReentrantLock();
            private final Condition outputWritable = outputLock.newCondition();
            private final Transformer t;
            private boolean notifyOutput = false;
            private State state = State.EMPTY;
            private final UnboundedContentHandlerBuffer in;
            private final UnboundedContentHandlerBuffer out;
            private static final EnumMap<LoggingErrorListener.ErrorLevel, Level> levelMap;
            private final LoggingErrorListener errorLogger = new LoggingErrorListener(logger, levelMap);
            private final SimpleLocalAbsoluteSAXXPath recordXPath = new SimpleLocalAbsoluteSAXXPath(SimpleLocalAbsoluteSAXXPath.INTEGRATOR_RECORD_XPATH);
            private final InputSource dummy = new InputSource();
            private final AtomicLong blockCount;

            static {
                levelMap = new EnumMap<LoggingErrorListener.ErrorLevel, Level>(LoggingErrorListener.ErrorLevel.class);
                levelMap.put(LoggingErrorListener.ErrorLevel.WARNING, Level.TRACE);
                levelMap.put(LoggingErrorListener.ErrorLevel.ERROR, Level.WARN);
                // FATAL_ERROR reported in subdivide
            }

            private TransformerRunner(Transformer t, UnboundedContentHandlerBuffer in, UnboundedContentHandlerBuffer out, AtomicLong blockCount) {
                this.blockCount = blockCount;
                subSplitter = new SplittingXMLFilter();
                subSplitter.setChunkSize(1);
                singleRecordInput.setUnmodifiableParent(subSplitter);
                singleRecordInput.setParentModifiable(false);
                this.t = t;
                t.setErrorListener(errorLogger);
                this.in = in;
                this.out = out;
                try {
                    synchronized (stf) {
                        th = stf.newTransformerHandler();
                    }
                    th.getTransformer().setOutputProperty(OutputKeys.INDENT, "yes");
                } catch (TransformerConfigurationException ex) {
                    throw new RuntimeException(ex);
                }
            }

            public void checkOut() {
                in.clear();
                out.clear();
                state = State.CHECKED_OUT;
            }

            public ContentHandler getInputBuffer() {
                return in;
            }

            @Override
            public void run() {
                SAXResult result = new SAXResult(out);
                result.setLexicalHandler(out);
                try {
                    t.transform(new SAXSource(in, dummy), result);
                } catch (TransformerException ex) {
                    try {
                        subdivide();
                    } catch (IOException ex1) {
                        throw new RuntimeException(ex1);
                    }
                }
                state = State.WRITABLE;
                if (notifyOutput) {
                    try {
                        outputLock.lock();
                        outputWritable.signal();
                    } finally {
                        outputLock.unlock();
                    }
                }
            }

            public void writeOutputTo(ContentHandler ch, LexicalHandler lh) throws SAXException, EOFException {
                if (state != State.WRITABLE) {
                    notifyOutput = true;
                    try {
                        outputLock.lock();
                        while (state != State.WRITABLE) {
                            if (state == State.EOF) {
                                throw new EOFException();
                            }
                            try {
                                blockCount.incrementAndGet();
                                outputWritable.await();
                            } catch (InterruptedException ex) {
                                throw new RuntimeException(ex);
                            }
                        }
                        notifyOutput = false;
                    } finally {
                        outputLock.unlock();
                    }
                }
                out.play(ch, lh);
                state = State.EMPTY;
            }

            public void notifyFinished() {
                if (state != State.EMPTY) {
                    throw new IllegalStateException();
                }
                state = State.EOF;
                if (notifyOutput) {
                    try {
                        outputLock.lock();
                        outputWritable.signal();
                    } finally {
                        outputLock.unlock();
                    }
                }
            }
            private final SplittingXMLFilter subSplitter;
            private UnboundedContentHandlerBuffer localOutputEventBuffer = new UnboundedContentHandlerBuffer();
            private UnboundedContentHandlerBuffer singleRecordInput = new UnboundedContentHandlerBuffer();

            private final TransformerHandler th;

            private void subdivide() throws IOException {
                out.clear();
                subSplitter.setParent(in);
                subSplitter.setContentHandler(singleRecordInput);
                try {
                    subSplitter.setProperty(LEXICAL_HANDLER_PROPERTY_KEY, singleRecordInput);
                } catch (SAXNotRecognizedException ex) {
                    throw new RuntimeException(ex);
                } catch (SAXNotSupportedException ex) {
                    throw new RuntimeException(ex);
                }
                localOutputEventBuffer.clear();
                do {
                    String errorMessage = null;
                    try {
                        singleRecordInput.clear();
                        subSplitter.parse(dummy);
                        t.transform(new SAXSource(singleRecordInput, dummy), new SAXResult(localOutputEventBuffer));
                        localOutputEventBuffer.flush(out, out);
                    } catch (SAXException ex) {
                        errorMessage = ex.toString();
                    } catch (TransformerException ex) {
                        errorMessage = ex.getMessageAndLocation();
                    }
                    if (errorMessage != null) {
                        boolean dumpRecord = true;
                        try {
                            logger.error("record " + recordXPath.evaluate(singleRecordInput, dummy) + "; " + errorMessage);
                            dumpRecord = false;
                        } catch (SAXException ex) {
                        } catch (IOException ex) {
                        }
                        if (dumpRecord) {
                            StringWriter sw = new StringWriter();
                            th.setResult(new StreamResult(sw));
                            try {
                                singleRecordInput.play(th, th);
                            } catch (SAXException ex) {
                                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                                PrintStream ps = new PrintStream(new ByteArrayOutputStream());
                                ps.append(ex.getMessage()+"; ");
                                try {
                                    singleRecordInput.dump(ps, true);
                                } catch (SAXException ex1) {
                                    throw new RuntimeException(ex1);
                                }
                                ps.flush();
                                sw = new StringWriter();
                                sw.append(baos.toString());
                            }
                            logger.error(errorMessage+"; "+sw.toString());
                        }
                        localOutputEventBuffer.clear();
                    }
                } while (subSplitter.hasMoreOutput(dummy));
            }
        }
    }
}
