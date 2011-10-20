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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.stream.StreamResult;
import org.apache.log4j.Logger;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
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

    private final Lock lock = new ReentrantLock();
    private boolean notifyInput = false;
    private boolean notifyOutput = false;
    private boolean notifyEmpty = false;
    private final Condition modified = lock.newCondition();
    private final Condition isEmpty = lock.newCondition();

    private final boolean useInputNotifyThreshold = true;
    private int inputNotifyThresholdCount = 0;
    private final boolean useOutputNotifyThreshold = true;
    private int outputNotifyThresholdCount = 0;
    private final int threshold;

    private int head = 0;
    private int tail = 0;
    private final SaxEventType[] events;
    private final boolean[] writable;
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

    private static Logger logger = Logger.getLogger(BoundedXMLFilterBuffer.class);
    public static final String TRANSFORMER_FACTORY_CLASS_NAME = "net.sf.saxon.TransformerFactoryImpl";

    public static void main(String[] args) throws FileNotFoundException, TransformerConfigurationException, ParserConfigurationException, SAXException, TransformerException, IOException {
        BufferedInputStream bis = new BufferedInputStream(new FileInputStream("/home/michael/NetBeansProjects/synch-branch/inputFiles/largest.xml"));
        BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream("/tmp/out.xml"));
        TransformerFactory tf = TransformerFactory.newInstance(TRANSFORMER_FACTORY_CLASS_NAME, null);
        Transformer t = tf.newTransformer();
        SAXParserFactory spf = SAXParserFactory.newInstance();
        spf.setNamespaceAware(true);
        BoundedXMLFilterBuffer buffer = new BoundedXMLFilterBuffer();
        XMLReader saxReader = spf.newSAXParser().getXMLReader();
        buffer.setParent(saxReader);
        long start = System.currentTimeMillis();
        t.transform(new SAXSource(buffer, new InputSource(bis)), new StreamResult(bos));
        bos.close();
        System.out.println("duration="+(System.currentTimeMillis() - start));
    }

    public BoundedXMLFilterBuffer() {
        bufferSize = DEFAULT_BUFFER_SIZE;
        threshold = (int) (bufferSize * 0.5);
        events = new SaxEventType[bufferSize];
        writable = new boolean[bufferSize];
        argIndex1 = new int[bufferSize];
        argIndex2 = new int[bufferSize];
        stringArgBuffer = new String[bufferSize * MAX_STRING_ARGS_PER_EVENT];
        attsArgBuffer = new Attributes[bufferSize];
        intArgBuffer = new int[bufferSize * MAX_INT_ARGS_PER_EVENT];
        charArgBuffer = new char[bufferSize * CHAR_BUFFER_INIT_FACTOR];
    }

    public BoundedXMLFilterBuffer(int bufferSize) {
        this.bufferSize = bufferSize;
        threshold = (int) (bufferSize * 0.5);
        events = new SaxEventType[bufferSize];
        writable = new boolean[bufferSize];
        argIndex1 = new int[bufferSize];
        argIndex2 = new int[bufferSize];
        stringArgBuffer = new String[bufferSize * MAX_STRING_ARGS_PER_EVENT];
        attsArgBuffer = new Attributes[bufferSize];
        intArgBuffer = new int[bufferSize * MAX_INT_ARGS_PER_EVENT];
        charArgBuffer = new char[bufferSize * CHAR_BUFFER_INIT_FACTOR];
    }

    public void clear() {
        head = 0;
        tail = 0;
        Arrays.fill(writable, false);
        charHead = 0;
        charTail = 0;
        charSize.set(0);
        stringTail = 0;
        attsTail = 0;
        intTail = 0;
        notifyInput = false;
        notifyOutput = false;
        notifyEmpty = false;
        outputNotifyThresholdCount = 0;
        inputNotifyThresholdCount = 0;
    }

    private void growCharArgBuffer() {
        notifyEmpty = true;
        try {
            lock.lock();
            if (notifyOutput) {
                modified.signal();
                outputNotifyThresholdCount = 0;
            }
            try {
                isEmpty.await();
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            }
            notifyEmpty = false;
        } finally {
            lock.unlock();
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
        if (writable[tail]) {
            notifyInput = true;
            try {
                lock.lock();
                while (writable[tail]) {
                    try {
                        modified.await();
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                }
                notifyInput = false;
            } finally {
                lock.unlock();
            }
        }
    }

    private void eventAdded() {
        writable[tail] = true;
        if (notifyOutput) {
            if (!useOutputNotifyThreshold || ++outputNotifyThresholdCount > threshold) {
                try {
                    lock.lock();
                    modified.signal();
                } finally {
                    lock.unlock();
                }
                outputNotifyThresholdCount = 0;
            }
        }
        tail = incrementMod(tail, bufferSize);
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
        outputNotifyThresholdCount = bufferSize;
        eventAdded();
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
        int charSizeSnapshot = charSize.get();
        while (charArgBuffer.length - charSizeSnapshot < length - 1) {
            growCharArgBuffer();
        }
        int charHeadSnapshot = charHead;
        if (charTail >= charHeadSnapshot && charArgBuffer.length - charTail < length) {
            firstChunkSize = charArgBuffer.length - charTail;
            System.arraycopy(ch, start, charArgBuffer, charTail, firstChunkSize);
            System.arraycopy(ch, start + firstChunkSize, charArgBuffer, 0, length - firstChunkSize);
        } else {
            try {
                System.arraycopy(ch, start, charArgBuffer, charTail, length);
            } catch (ArrayIndexOutOfBoundsException ex) {
                System.out.println(ch.length+", "+start+", "+charArgBuffer.length+", "+charTail+", "+length+", head="+charHeadSnapshot + ", size="+charSizeSnapshot);
                throw ex;
            }
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
        Thread t = new Thread(new EventPlayer(), "eventPlayer<-"+Thread.currentThread().getName());
        t.start();
        super.parse(input);
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
            while (parsing) {
                if (!writable[head]) {
                    notifyOutput = true;
                    try {
                        lock.lock();
                        while (!writable[head]) {
                            if (notifyEmpty) {
                                isEmpty.signal();
                            }
                            try {
                                modified.await();
                            } catch (InterruptedException ex) {
                                throw new RuntimeException(ex);
                            }
                        }
                        notifyOutput = false;
                    } finally {
                        lock.unlock();
                    }
                }
                try {
                    execute(head);
                } catch (SAXException ex) {
                    throw new RuntimeException(ex);
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
                parsing = false;
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
        writable[head] = false;
        if (notifyInput) {
            if (!useInputNotifyThreshold || ++inputNotifyThresholdCount > threshold) {
                try {
                    lock.lock();
                    modified.signal();
                } finally {
                    lock.unlock();
                }
                inputNotifyThresholdCount = 0;
            }
        }
        head = incrementMod(head, bufferSize);
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
