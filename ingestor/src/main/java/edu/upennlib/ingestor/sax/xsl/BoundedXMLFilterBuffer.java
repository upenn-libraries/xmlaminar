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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.log4j.Logger;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;
import org.xml.sax.helpers.XMLFilterImpl;

/**
 *
 * @author michael
 */
public class BoundedXMLFilterBuffer extends XMLFilterImpl {

    private static final int MAX_STRING_ARGS_PER_EVENT = 3;
    private static final int MAX_INT_ARGS_PER_EVENT = 2;
    private static final int CHAR_BUFFER_INIT_FACTOR = 6;
    public static final int DEFAULT_BUFFER_SIZE = 2000;
    private final int bufferSize;
    private final boolean useInputThreshold = true;
    private final boolean useOutputThreshold = true;
    private final int threshold;
    private final int tAdd = 1;

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notFull = lock.newCondition();
    private final Condition notEmpty = lock.newCondition();
    private final AtomicBoolean activeIn = new AtomicBoolean(false);
    private final AtomicBoolean activeOut = new AtomicBoolean(false);

    private final AtomicInteger size = new AtomicInteger(0);
    private int head = 0;
    private int tail = 0;
    private final SaxEventType[] events;
    private final int[] argIndex1;
    private final int[] argIndex2;

    private final String[] stringArgBuffer;
    private int stringTail = 0;

    private final Attributes[] attsArgBuffer;
    private int attsTail = 0;

    private final int[] intArgBuffer;
    private int intTail = 0;

    private char[] charArgBuffer;
    private final AtomicInteger charSize = new AtomicInteger(0);
    private int charHead = 0;
    private int charTail = 0;

    private Logger logger = Logger.getLogger(getClass());

    public BoundedXMLFilterBuffer() {
        bufferSize = DEFAULT_BUFFER_SIZE;
        threshold = (bufferSize / 2);
        //tAdd = (bufferSize / 4);
        events = new SaxEventType[bufferSize];
        argIndex1 = new int[bufferSize];
        argIndex2 = new int[bufferSize];
        stringArgBuffer = new String[bufferSize * MAX_STRING_ARGS_PER_EVENT];
        attsArgBuffer = new Attributes[bufferSize];
        intArgBuffer = new int[bufferSize * MAX_INT_ARGS_PER_EVENT];
        charArgBuffer = new char[bufferSize * CHAR_BUFFER_INIT_FACTOR];
    }

    public BoundedXMLFilterBuffer(int bufferSize) {
        this.bufferSize = bufferSize;
        threshold = (bufferSize / 2);
        //tAdd = (bufferSize / 4);
        events = new SaxEventType[bufferSize];
        argIndex1 = new int[bufferSize];
        argIndex2 = new int[bufferSize];
        stringArgBuffer = new String[bufferSize * MAX_STRING_ARGS_PER_EVENT];
        attsArgBuffer = new Attributes[bufferSize];
        intArgBuffer = new int[bufferSize * MAX_INT_ARGS_PER_EVENT];
        charArgBuffer = new char[bufferSize * CHAR_BUFFER_INIT_FACTOR];
    }

    private void growCharArgBuffer() {
        if (size.get() > 0) {
            activeIn.set(false);
            try {
                lock.lock();
                if (useInputThreshold && !activeOut.get()) {
                    notEmpty.signal();
                }
                while (size.get() > 0) {
                    notEmpty.await();
                }
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            } finally {
                lock.unlock();
                activeIn.set(true);
            }
        }
        System.out.println("increasing charArgBuffer size to: " + (charArgBuffer.length * 2));
        charArgBuffer = new char[charArgBuffer.length * 2];
        charHead = 0;
        charTail = 0;
        charSize.set(0);
    }

    /*
     * Buffering
     */
    private void blockForSpace() {
        if (size.get() >= bufferSize) {
            activeIn.set(false);
            try {
                lock.lock();
                while (size.get() >= bufferSize) {
                    notFull.await();
                }
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            } finally {
                lock.unlock();
                activeIn.set(true);
            }
        }
    }

    private void eventAdded() {
        tail = incrementMod(tail, bufferSize);
        size.incrementAndGet();
        if ((!useInputThreshold || size.get() > threshold - tAdd) && !activeOut.get()) {
            try {
                lock.lock();
                notEmpty.signal();
            } finally {
                lock.unlock();
            }
        }
    }

    @Override
    public void startDocument() throws SAXException {
        blockForSpace();
        events[tail] = SaxEventType.startDocument;
        eventAdded();
    }

    @Override
    public void endDocument() throws SAXException {
        blockForSpace();
        events[tail] = SaxEventType.endDocument;
        eventAdded();
        if (useInputThreshold && !activeOut.get()) {
            try {
                lock.lock();
                notEmpty.signal();
            } finally {
                lock.unlock();
            }
        }
    }

    @Override
    public void startPrefixMapping(String prefix, String uri) throws SAXException {
        blockForSpace();
        argIndex1[tail] = stringTail;
        stringArgBuffer[stringTail] = prefix;
        stringTail = incrementMod(stringTail, stringArgBuffer.length);
        stringArgBuffer[stringTail] = uri;
        stringTail = incrementMod(stringTail, stringArgBuffer.length);
        events[tail] = SaxEventType.startPrefixMapping;
        eventAdded();
    }

    @Override
    public void endPrefixMapping(String prefix) throws SAXException {
        blockForSpace();
        argIndex1[tail] = stringTail;
        stringArgBuffer[stringTail] = prefix;
        stringTail = incrementMod(stringTail, stringArgBuffer.length);
        events[tail] = SaxEventType.endPrefixMapping;
        eventAdded();
    }

    @Override
    public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
        blockForSpace();
        argIndex1[tail] = stringTail;
        stringArgBuffer[stringTail] = uri;
        stringTail = incrementMod(stringTail, stringArgBuffer.length);
        stringArgBuffer[stringTail] = localName;
        stringTail = incrementMod(stringTail, stringArgBuffer.length);
        stringArgBuffer[stringTail] = qName;
        stringTail = incrementMod(stringTail, stringArgBuffer.length);
        argIndex2[tail] = attsTail;
        attsArgBuffer[attsTail] = new AttributesImpl(atts);
        attsTail = incrementMod(attsTail, attsArgBuffer.length);
        events[tail] = SaxEventType.startElement;
        eventAdded();
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        blockForSpace();
        argIndex1[tail] = stringTail;
        stringArgBuffer[stringTail] = uri;
        stringTail = incrementMod(stringTail, stringArgBuffer.length);
        stringArgBuffer[stringTail] = localName;
        stringTail = incrementMod(stringTail, stringArgBuffer.length);
        stringArgBuffer[stringTail] = qName;
        stringTail = incrementMod(stringTail, stringArgBuffer.length);
        events[tail] = SaxEventType.endElement;
        eventAdded();
    }

    private int firstChunkSize;

    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        blockForSpace();
        argIndex1[tail] = intTail;
        while (charArgBuffer.length - charSize.get() < length - 1) {
            growCharArgBuffer();
        }
        if (charTail >= charHead && charArgBuffer.length - charTail < length) {
            firstChunkSize = charArgBuffer.length - charTail;
            System.arraycopy(ch, start, charArgBuffer, charTail, firstChunkSize);
            System.arraycopy(ch, start + firstChunkSize, charArgBuffer, 0, length - firstChunkSize);
        } else {
            System.arraycopy(ch, start, charArgBuffer, charTail, length);
        }
        intArgBuffer[intTail] = charTail;
        intTail = incrementMod(intTail, intArgBuffer.length);
        charTail = simpleMod(charTail + length, charArgBuffer.length);
        intArgBuffer[intTail] = length;
        intTail = incrementMod(intTail, intArgBuffer.length);
        charSize.addAndGet(length);
        events[tail] = SaxEventType.characters;
        eventAdded();
    }

    @Override
    public void ignorableWhitespace(char[] ch, int start, int length) throws SAXException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void processingInstruction(String target, String data) throws SAXException {
        blockForSpace();
        argIndex1[tail] = stringTail;
        stringArgBuffer[stringTail] = target;
        stringTail = incrementMod(stringTail, stringArgBuffer.length);
        stringArgBuffer[stringTail] = data;
        stringTail = incrementMod(stringTail, stringArgBuffer.length);
        events[tail] = SaxEventType.processingInstruction;
        eventAdded();
    }

    @Override
    public void skippedEntity(String name) throws SAXException {
        throw new UnsupportedOperationException("Not supported.");
    }

    private int incrementMod(int subject, int mod) {
        int toReturn;
        if ((toReturn = subject + 1) == mod) {
            return 0;
        } else {
            return toReturn;
        }
    }

    private int simpleMod(int subject, int mod) {
        if (subject >= mod) {
            return subject - mod;
        } else {
            return subject;
        }
    }

    /*
     * Execution
     */

    private boolean parsing = false;

    @Override
    public void parse(InputSource input) throws SAXException, IOException {
        if (parsing) {
            throw new IllegalStateException();
        }
        parsing = true;
        Thread t = new Thread(new EventPlayer(), "eventPlayerThread");
        t.start();
        super.parse(input);
        parsing = false;
        try {
            t.join();
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void parse(String systemId) throws SAXException, IOException {
        throw new UnsupportedOperationException("not supported.");
    }

    private class EventPlayer implements Runnable {

        @Override
        public void run() {
            while (parsing || size.get() > 0) {
                if (size.get() > 0) {
                    activeOut.set(true);
                    try {
                        execute(head);
                    } catch (SAXException ex) {
                        throw new RuntimeException(ex);
                    }
                } else {
                    activeOut.set(false);
                    try {
                        lock.lock();
                        while (parsing && size.get() < 1) {
                            notEmpty.await();
                        }
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    } finally {
                        lock.unlock();
                    }
                }
            }
        }
    }

    private void execute(int index) throws SAXException {
        switch (events[index]) {
            case startDocument:
                super.startDocument();
                break;
            case endDocument:
                super.endDocument();
                break;
            case startPrefixMapping:
                executeStartPrefixMapping(index);
                break;
            case endPrefixMapping:
                executeEndPrefixMapping(index);
                break;
            case startElement:
                executeStartElement(index);
                break;
            case endElement:
                executeEndElement(index);
                break;
            case characters:
                executeCharacters(index);
                break;
            case ignorableWhitespace:
                executeIgnorableWhitespace(index);
        }
        eventExecuted();
    }

    private void eventExecuted() {
        head = incrementMod(head, bufferSize);
        size.decrementAndGet();
        if ((!useOutputThreshold || size.get() < threshold + tAdd) && !activeIn.get()) {
            try {
                lock.lock();
                notFull.signal();
            } finally {
                lock.unlock();
            }
        }
    }
    private int ind1;
    private int ind2;
    private int ind3;
    private int ind4;

    private void executeStartPrefixMapping(int index) throws SAXException {
        ind1 = argIndex1[index];
        ind2 = incrementMod(ind1, stringArgBuffer.length);
        super.startPrefixMapping(stringArgBuffer[ind1], stringArgBuffer[ind2]);
    }

    private void executeEndPrefixMapping(int index) throws SAXException {
        ind1 = argIndex1[index];
        super.endPrefixMapping(stringArgBuffer[ind1]);
    }

    private void executeStartElement(int index) throws SAXException {
        ind1 = argIndex1[index];
        ind2 = incrementMod(ind1, stringArgBuffer.length);
        ind3 = incrementMod(ind2, stringArgBuffer.length);
        ind4 = argIndex2[index];
        super.startElement(stringArgBuffer[ind1], stringArgBuffer[ind2], stringArgBuffer[ind3], attsArgBuffer[ind4]);
    }

    private void executeEndElement(int index) throws SAXException {
        ind1 = argIndex1[index];
        ind2 = incrementMod(ind1, stringArgBuffer.length);
        ind3 = incrementMod(ind2, stringArgBuffer.length);
        super.endElement(stringArgBuffer[ind1], stringArgBuffer[ind2], stringArgBuffer[ind3]);
    }

    private int executeFirstChunkSize;

    private void executeCharacters(int index) throws SAXException {
        ind1 = argIndex1[index];
        ind2 = incrementMod(ind1, intArgBuffer.length);
        if (intArgBuffer[ind2] > charArgBuffer.length - intArgBuffer[ind1]) {
            executeFirstChunkSize = charArgBuffer.length - intArgBuffer[ind1];
            super.characters(charArgBuffer, intArgBuffer[ind1], executeFirstChunkSize);
            super.characters(charArgBuffer, 0, intArgBuffer[ind2] - executeFirstChunkSize);
        } else {
            super.characters(charArgBuffer, intArgBuffer[ind1], intArgBuffer[ind2]);
        }
        charHead = simpleMod(charHead + intArgBuffer[ind2], charArgBuffer.length);
        charSize.addAndGet(-intArgBuffer[ind2]);
    }

    private void executeIgnorableWhitespace(int index) throws SAXException {
        throw new UnsupportedOperationException();
    }

}
