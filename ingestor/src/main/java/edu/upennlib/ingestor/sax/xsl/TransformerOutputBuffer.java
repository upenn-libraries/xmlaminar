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

package edu.upennlib.ingestor.sax.xsl;

import java.io.EOFException;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.xml.transform.Templates;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.sax.SAXResult;
import javax.xml.transform.sax.SAXSource;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXNotSupportedException;
import org.xml.sax.ext.LexicalHandler;

/**
 *
 * @author michael
 */
public class TransformerOutputBuffer implements Runnable {

    public static enum State { EMPTY, CHECKED_OUT, WRITABLE, EOF };
    private final int size;
    private final TransformerRunner[] trs;
    private int head = 0;
    private int tail = 0;
    private final Lock inputLock = new ReentrantLock();
    private final Condition spaceAvailable = inputLock.newCondition();
    private long blockInput = 0;
    private AtomicLong blockOutput = new AtomicLong(0);
    private boolean notifyInput = false;
    private final ContentHandler output;

    public TransformerOutputBuffer(int size, ContentHandler output, Templates temp) throws TransformerConfigurationException {
        this.size = size;
        this.output = output;
        trs = new TransformerRunner[size];
        for (int i = 0; i < size; i++) {
            trs[i] = new TransformerRunner(temp.newTransformer(), new UnboundedContentHandlerBuffer(), new UnboundedContentHandlerBuffer(), blockOutput);
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
        //tail = (tail + 1) % size;
    }

    @Override
    public void run() {
        try {
            while (true) {
                try {
                    trs[head].writeOutputTo(output);
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
        System.out.println("blockInput="+blockInput);
        System.out.println("blockOutput="+blockOutput.get());
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

        public ContentHandler getInputHandler() {
            return in;
        }

        @Override
        public void run() {
            SAXResult result = new SAXResult(out);
            //result.setLexicalHandler(out);
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

        public void writeOutputTo(ContentHandler ch) throws SAXException, EOFException {
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
            out.play(ch);
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

        private final UnboundedContentHandlerBuffer individualInputBuffer = new UnboundedContentHandlerBuffer();
        private final SplittingXMLFilter subSplitter;
        private UnboundedContentHandlerBuffer localOutputEventBuffer = new UnboundedContentHandlerBuffer();

        private void subdivide(Transformer t, UnboundedContentHandlerBuffer in, UnboundedContentHandlerBuffer out, InputSource dummyInput) {
            out.clear();
            individualInputBuffer.clear();
            subSplitter.setParent(in);
            subSplitter.setContentHandler(individualInputBuffer);
            if (individualInputBuffer instanceof LexicalHandler) {
                try {
                    subSplitter.setProperty("http://xml.org/sax/properties/lexical-handler", individualInputBuffer);
                } catch (SAXNotRecognizedException ex) {
                    throw new RuntimeException(ex);
                } catch (SAXNotSupportedException ex) {
                    throw new RuntimeException(ex);
                }
            }
            do {
                try {
                    subSplitter.parse(dummyInput);
                    t.transform(new SAXSource(individualInputBuffer, new InputSource()), new SAXResult(localOutputEventBuffer));
                    localOutputEventBuffer.flush(out);
                } catch (SAXException ex) {
                    localOutputEventBuffer.clear();
                } catch (TransformerException ex) {
                    localOutputEventBuffer.clear();
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            } while (subSplitter.hasMoreOutput(dummyInput));
        }

    }
}
