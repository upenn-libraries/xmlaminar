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
import org.apache.log4j.Logger;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.AttributesImpl;

/**
 *
 * @author michael
 */
public class UnboundedContentHandlerBuffer implements ContentHandler, XMLReader {

    private static final int STRING_ARGS_INIT_FACTOR = 3;
    private static final int INT_ARGS_INIT_FACTOR = 2;
    private static final int CHAR_BUFFER_INIT_FACTOR = 6;
    public static final int INITIAL_BUFFER_SIZE = 1024;

    private int size = 0;
    private int tail = 0;
    private final SaxEventType[] events;
    private int[] argIndex1;
    private int[] argIndex2;

    private String[] stringArgBuffer;
    private int stringTail = 0;

    private Attributes[] attsArgBuffer;
    private int attsTail = 0;

    private int[] intArgBuffer;
    private int intTail = 0;

    private char[] charArgBuffer;
    private int charTail = 0;

    private Logger logger = Logger.getLogger(getClass());

    public UnboundedContentHandlerBuffer() {
        int initialBufferSize = INITIAL_BUFFER_SIZE;
        events = new SaxEventType[initialBufferSize];
        argIndex1 = new int[initialBufferSize];
        argIndex2 = new int[initialBufferSize];
        stringArgBuffer = new String[initialBufferSize * STRING_ARGS_INIT_FACTOR];
        attsArgBuffer = new Attributes[initialBufferSize];
        intArgBuffer = new int[initialBufferSize * INT_ARGS_INIT_FACTOR];
        charArgBuffer = new char[initialBufferSize * CHAR_BUFFER_INIT_FACTOR];
    }

    public UnboundedContentHandlerBuffer(int initialBufferSize) {
        events = new SaxEventType[initialBufferSize];
        argIndex1 = new int[initialBufferSize];
        argIndex2 = new int[initialBufferSize];
        stringArgBuffer = new String[initialBufferSize * STRING_ARGS_INIT_FACTOR];
        attsArgBuffer = new Attributes[initialBufferSize];
        intArgBuffer = new int[initialBufferSize * INT_ARGS_INIT_FACTOR];
        charArgBuffer = new char[initialBufferSize * CHAR_BUFFER_INIT_FACTOR];
    }

    private void checkSpace(int stringSpace, int attsSpace, int intSpace, int charSpace) {

    }

    /*
     * Buffering
     */
    @Override
    public void startDocument() throws SAXException {
        checkSpace(0, 0, 0, 0);
        events[tail++] = SaxEventType.startDocument;
    }

    @Override
    public void endDocument() throws SAXException {
        checkSpace(0, 0, 0, 0);
        events[tail++] = SaxEventType.endDocument;
    }

    @Override
    public void startPrefixMapping(String prefix, String uri) throws SAXException {
        checkSpace(2, 0, 0, 0);
        argIndex1[tail] = stringTail;
        stringArgBuffer[stringTail++] = prefix;
        stringArgBuffer[stringTail++] = uri;
        events[tail++] = SaxEventType.startPrefixMapping;
    }

    @Override
    public void endPrefixMapping(String prefix) throws SAXException {
        checkSpace(1, 0, 0, 0);
        argIndex1[tail] = stringTail;
        stringArgBuffer[stringTail++] = prefix;
        events[tail++] = SaxEventType.endPrefixMapping;
    }

    @Override
    public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
        checkSpace(3, 1, 0, 0);
        argIndex1[tail] = stringTail;
        stringArgBuffer[stringTail++] = uri;
        stringArgBuffer[stringTail++] = localName;
        stringArgBuffer[stringTail++] = qName;
        argIndex2[tail] = attsTail;
        attsArgBuffer[attsTail++] = new AttributesImpl(atts);
        events[tail++] = SaxEventType.startElement;
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        checkSpace(3, 0, 0, 0);
        argIndex1[tail] = stringTail;
        stringArgBuffer[stringTail++] = uri;
        stringArgBuffer[stringTail++] = localName;
        stringArgBuffer[stringTail++] = qName;
        events[tail++] = SaxEventType.endElement;
    }

    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        checkSpace(0, 0, 1, length);
        argIndex1[tail] = intTail;
        intArgBuffer[intTail++] = charTail;
        intArgBuffer[intTail++] = length;
        System.arraycopy(ch, start, charArgBuffer, charTail, length);
        charTail += length;
        events[tail++] = SaxEventType.characters;
    }

    @Override
    public void ignorableWhitespace(char[] ch, int start, int length) throws SAXException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void processingInstruction(String target, String data) throws SAXException {
        checkSpace(2, 0, 0, 0);
        argIndex1[tail] = stringTail;
        stringArgBuffer[stringTail++] = target;
        stringArgBuffer[stringTail++] = data;
        events[tail++] = SaxEventType.processingInstruction;
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
        for (int i = 0; i < tail; i++) {
            execute(i, getContentHandler());
        }
        parsing = false;
    }

    @Override
    public void parse(String systemId) throws SAXException, IOException {
        throw new UnsupportedOperationException("not supported.");
    }

    private void execute(int index, ContentHandler ch) throws SAXException {
        switch (events[index]) {
            case startDocument:
                ch.startDocument();
                break;
            case endDocument:
                ch.endDocument();
                break;
            case startPrefixMapping:
                executeStartPrefixMapping(index, ch);
                break;
            case endPrefixMapping:
                executeEndPrefixMapping(index, ch);
                break;
            case startElement:
                executeStartElement(index, ch);
                break;
            case endElement:
                executeEndElement(index, ch);
                break;
            case characters:
                executeCharacters(index, ch);
                break;
            case ignorableWhitespace:
                executeIgnorableWhitespace(index, ch);
        }
    }

    private int indType1;
    private int indType2;

    private void executeStartPrefixMapping(int index, ContentHandler ch) throws SAXException {
        indType1 = argIndex1[index];
        ch.startPrefixMapping(stringArgBuffer[indType1], stringArgBuffer[indType1 + 1]);
    }

    private void executeEndPrefixMapping(int index, ContentHandler ch) throws SAXException {
        indType1 = argIndex1[index];
        ch.endPrefixMapping(stringArgBuffer[indType1]);
    }

    private void executeStartElement(int index, ContentHandler ch) throws SAXException {
        indType1 = argIndex1[index];
        indType2 = argIndex2[index];
        ch.startElement(stringArgBuffer[indType1], stringArgBuffer[indType1 + 1], stringArgBuffer[indType1 + 2], attsArgBuffer[indType2]);
    }

    private void executeEndElement(int index, ContentHandler ch) throws SAXException {
        indType1 = argIndex1[index];
        ch.endElement(stringArgBuffer[indType1], stringArgBuffer[indType1 + 1], stringArgBuffer[indType1 + 2]);
    }

    private void executeCharacters(int index, ContentHandler ch) throws SAXException {
        indType1 = argIndex1[index];
        ch.characters(charArgBuffer, intArgBuffer[indType1], intArgBuffer[indType1 + 1]);
    }

    private void executeIgnorableWhitespace(int index, ContentHandler ch) throws SAXException {
        throw new UnsupportedOperationException();
    }

}
