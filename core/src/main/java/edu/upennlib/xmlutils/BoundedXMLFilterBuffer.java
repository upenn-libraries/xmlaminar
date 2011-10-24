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

package edu.upennlib.xmlutils;

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

/**
 *
 * @author michael
 */
public class BoundedXMLFilterBuffer extends XMLFilterLexicalHandlerImpl {

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

    private static final Logger logger = Logger.getLogger(BoundedXMLFilterBuffer.class);

    public static void main(String[] args) throws FileNotFoundException, TransformerConfigurationException, ParserConfigurationException, SAXException, TransformerException, IOException {
        BufferedInputStream bis = new BufferedInputStream(new FileInputStream("/home/michael/NetBeansProjects/synch-branch/inputFiles/largest.xml"));
        BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream("/tmp/out.xml"));
        TransformerFactory tf = TransformerFactory.newInstance();
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
    public void endDTD() throws SAXException {
        blockForSpace();
        events[tail] = SaxEventType.endDTD;
        eventAdded();
    }

    @Override
    public void startCDATA() throws SAXException {
        blockForSpace();
        events[tail] = SaxEventType.startCDATA;
        eventAdded();
    }

    @Override
    public void endCDATA() throws SAXException {
        blockForSpace();
        events[tail] = SaxEventType.endCDATA;
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
        bufferStringArgs(prefix, uri);
        events[tail] = SaxEventType.startPrefixMapping;
        eventAdded();
    }

    @Override
    public void endPrefixMapping(String prefix) throws SAXException {
        blockForSpace();
        bufferStringArgs(prefix);
        events[tail] = SaxEventType.endPrefixMapping;
        eventAdded();
    }

    @Override
    public void startEntity(String name) throws SAXException {
        blockForSpace();
        bufferStringArgs(name);
        events[tail] = SaxEventType.startEntity;
        eventAdded();
    }

    @Override
    public void endEntity(String name) throws SAXException {
        blockForSpace();
        bufferStringArgs(name);
        events[tail] = SaxEventType.endEntity;
        eventAdded();
    }

    @Override
    public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
        blockForSpace();
        bufferStringArgs(uri, localName, qName);
        argIndex2[tail] = attsTail;
        attsArgBuffer[attsTail] = new AttributesImpl(atts);
        attsTail = incrementMod(attsTail, attsArgBuffer.length);
        events[tail] = SaxEventType.startElement;
        eventAdded();
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        blockForSpace();
        bufferStringArgs(uri, localName, qName);
        events[tail] = SaxEventType.endElement;
        eventAdded();
    }

    @Override
    public void startDTD(String name, String publicId, String systemId) throws SAXException {
        blockForSpace();
        bufferStringArgs(name, publicId, systemId);
        events[tail] = SaxEventType.startDTD;
        eventAdded();
    }

    private void bufferStringArgs(String one) {
        argIndex1[tail] = stringTail;
        stringArgBuffer[stringTail] = one;
        stringTail = incrementMod(stringTail, stringArgBuffer.length);
    }

    private void bufferStringArgs(String one, String two) {
        argIndex1[tail] = stringTail;
        stringArgBuffer[stringTail] = one;
        stringTail = incrementMod(stringTail, stringArgBuffer.length);
        stringArgBuffer[stringTail] = two;
        stringTail = incrementMod(stringTail, stringArgBuffer.length);
    }

    private void bufferStringArgs(String one, String two, String three) {
        argIndex1[tail] = stringTail;
        stringArgBuffer[stringTail] = one;
        stringTail = incrementMod(stringTail, stringArgBuffer.length);
        stringArgBuffer[stringTail] = two;
        stringTail = incrementMod(stringTail, stringArgBuffer.length);
        stringArgBuffer[stringTail] = three;
        stringTail = incrementMod(stringTail, stringArgBuffer.length);
    }

    private int firstChunkSize;

    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        blockForSpace();
        bufferCharArgs(ch, start, length);
        events[tail] = SaxEventType.characters;
        eventAdded();
    }

    @Override
    public void ignorableWhitespace(char[] ch, int start, int length) throws SAXException {
        blockForSpace();
        bufferCharArgs(ch, start, length);
        events[tail] = SaxEventType.ignorableWhitespace;
        eventAdded();
    }

    @Override
    public void comment(char[] ch, int start, int length) throws SAXException {
        blockForSpace();
        bufferCharArgs(ch, start, length);
        events[tail] = SaxEventType.comment;
        eventAdded();
    }

    private void bufferCharArgs(char[] ch, int start, int length) {
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
    }

    @Override
    public void processingInstruction(String target, String data) throws SAXException {
        blockForSpace();
        bufferStringArgs(target, data);
        events[tail] = SaxEventType.processingInstruction;
        eventAdded();
    }

    @Override
    public void skippedEntity(String name) throws SAXException {
        blockForSpace();
        bufferStringArgs(name);
        events[tail] = SaxEventType.skippedEntity;
        eventAdded();
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
        parse(input, null);
    }

    @Override
    public void parse(String systemId) throws SAXException, IOException {
        parse(null, systemId);
    }

    private void parse(InputSource input, String systemId) throws SAXException, IOException {
        if (parsing) {
            throw new IllegalStateException();
        }
        parsing = true;
        Thread t = new Thread(new EventPlayer(), "eventPlayer<-"+Thread.currentThread().getName());
        t.start();
        getParent().setProperty(LEXICAL_HANDLER_PROPERTY_KEY, this);
        if (input != null) {
            super.parse(input);
        } else if (systemId != null) {
            super.parse(systemId);
        } else {
            throw new IllegalArgumentException("null argument to parse()");
        }
        try {
            t.join();
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
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

    private int index1;
    private int index2;
    private int index3;
    private int index4;

    private void execute(int index) throws SAXException {
        SaxEventType type = events[index];
        switch (type) {
            case startDocument:
                super.startDocument();
                break;
            case endDocument:
                super.endDocument();
                parsing = false;
                break;
            case startPrefixMapping:
                index1 = argIndex1[index];
                index2 = incrementMod(index1, stringArgBuffer.length);
                super.startPrefixMapping(stringArgBuffer[index1], stringArgBuffer[index2]);
                break;
            case endPrefixMapping:
                index1 = argIndex1[index];
                super.endPrefixMapping(stringArgBuffer[index1]);
                break;
            case startElement:
                index1 = argIndex1[index];
                index2 = incrementMod(index1, stringArgBuffer.length);
                index3 = incrementMod(index2, stringArgBuffer.length);
                index4 = argIndex2[index];
                super.startElement(stringArgBuffer[index1], stringArgBuffer[index2], stringArgBuffer[index3], attsArgBuffer[index4]);
                break;
            case endElement:
                index1 = argIndex1[index];
                index2 = incrementMod(index1, stringArgBuffer.length);
                index3 = incrementMod(index2, stringArgBuffer.length);
                super.endElement(stringArgBuffer[index1], stringArgBuffer[index2], stringArgBuffer[index3]);
                break;
            case characters:
                executeCharArgs(type, index);
                break;
            case ignorableWhitespace:
                executeCharArgs(type, index);
                break;
            case startDTD:
                index1 = argIndex1[index];
                index2 = incrementMod(index1, stringArgBuffer.length);
                index3 = incrementMod(index2, stringArgBuffer.length);
                super.startDTD(stringArgBuffer[index1], stringArgBuffer[index2], stringArgBuffer[index3]);
                break;
            case endDTD:
                super.endDTD();
                break;
            case startEntity:
                index1 = argIndex1[index];
                super.startEntity(stringArgBuffer[index1]);
                break;
            case endEntity:
                index1 = argIndex1[index];
                super.endEntity(stringArgBuffer[index1]);
                break;
            case startCDATA:
                super.startCDATA();
                break;
            case endCDATA:
                super.endCDATA();
                break;
            case comment:
                executeCharArgs(type, index);
                break;
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

    private int executeFirstChunkSize;

    private void executeCharArgs(SaxEventType type, int index) throws SAXException {
        index1 = argIndex1[index];
        index2 = incrementMod(index1, intArgBuffer.length);
        if (intArgBuffer[index2] > charArgBuffer.length - intArgBuffer[index1]) {
            executeFirstChunkSize = charArgBuffer.length - intArgBuffer[index1];
            switch (type) {
                case characters:
                    super.characters(charArgBuffer, intArgBuffer[index1], executeFirstChunkSize);
                    super.characters(charArgBuffer, 0, intArgBuffer[index2] - executeFirstChunkSize);
                    break;
                case ignorableWhitespace:
                    super.ignorableWhitespace(charArgBuffer, intArgBuffer[index1], executeFirstChunkSize);
                    super.ignorableWhitespace(charArgBuffer, 0, intArgBuffer[index2] - executeFirstChunkSize);
                    break;
                case comment:
                    super.comment(charArgBuffer, intArgBuffer[index1], executeFirstChunkSize);
                    super.comment(charArgBuffer, 0, intArgBuffer[index2] - executeFirstChunkSize);
                    break;
            }
        } else {
            switch (type) {
                case characters:
                    super.characters(charArgBuffer, intArgBuffer[index1], intArgBuffer[index2]);
                    break;
                case ignorableWhitespace:
                    super.ignorableWhitespace(charArgBuffer, intArgBuffer[index1], intArgBuffer[index2]);
                    break;
                case comment:
                    super.comment(charArgBuffer, intArgBuffer[index1], intArgBuffer[index2]);
                    break;
            }
        }
        charHead = simpleMod(charHead + intArgBuffer[index2], charArgBuffer.length);
        charSize.addAndGet(-intArgBuffer[index2]);
    }

}
