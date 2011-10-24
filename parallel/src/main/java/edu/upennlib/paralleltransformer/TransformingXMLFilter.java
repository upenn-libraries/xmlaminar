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
import edu.upennlib.xmlutils.SplittingXMLFilter;
import edu.upennlib.xmlutils.UnboundedContentHandlerBuffer;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.Templates;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.sax.SAXResult;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.sax.SAXTransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import net.sf.saxon.Controller;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXNotSupportedException;
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
    private Templates stylesheetTemplates;
    public static final String TRANSFORMER_FACTORY_CLASS_NAME = "net.sf.saxon.TransformerFactoryImpl";
    private static final SAXTransformerFactory stf = (SAXTransformerFactory) TransformerFactory.newInstance(TRANSFORMER_FACTORY_CLASS_NAME, null);

    public static void main(String[] args) throws SAXException, ParserConfigurationException, FileNotFoundException, TransformerConfigurationException, TransformerException, IOException {
        File stylesheet = new File("/tmp/franklin_nsaware.xsl");
        File inputFile = new File("/home/michael/NetBeansProjects/synch-branch/inputFiles/large.xml");
        File outputFile = new File("/tmp/large_transform.xml");

        InputSource inputSource = new InputSource(new BufferedInputStream(new FileInputStream(inputFile)));
        BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(outputFile));

        TransformingXMLFilter txf = new TransformingXMLFilter();
        txf.setStylesheet(stylesheet);

        Transformer t = stf.newTransformer();
        txf.configureOutputTransformer((Controller) t);

        long start = System.currentTimeMillis();
        t.transform(new SAXSource(txf, inputSource), new StreamResult(bos));
        bos.close();
        System.out.println("duration: " + (System.currentTimeMillis() - start));
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

    public void setStylesheet(File stylesheet) throws TransformerConfigurationException {
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
        if (stylesheet != null) {
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
            private final InputSource dummy = new InputSource();
            private final AtomicLong blockCount;

            private TransformerRunner(Transformer t, UnboundedContentHandlerBuffer in, UnboundedContentHandlerBuffer out, AtomicLong blockCount) {
                this.blockCount = blockCount;
                subSplitter = new SplittingXMLFilter();
                subSplitter.setChunkSize(1);
                this.t = t;
                this.in = in;
                this.out = out;
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
                    subdivide(t, in, out, dummy);
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

            private void subdivide(Transformer t, UnboundedContentHandlerBuffer in, UnboundedContentHandlerBuffer out, InputSource dummyInput) {
                out.clear();
                subSplitter.setParent(in);
                do {
                    try {
                        t.transform(new SAXSource(subSplitter, dummyInput), new SAXResult(localOutputEventBuffer));
                        localOutputEventBuffer.flush(out, out);
                    } catch (SAXException ex) {
                        localOutputEventBuffer.clear();
                    } catch (TransformerException ex) {
                        localOutputEventBuffer.clear();
                    }
                } while (subSplitter.hasMoreOutput(dummyInput));
            }
        }
    }
}
