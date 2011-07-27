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
import java.io.PrintStream;
import java.io.PrintWriter;
import org.apache.log4j.Logger;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.DTDHandler;
import org.xml.sax.EntityResolver;
import org.xml.sax.ErrorHandler;
import org.xml.sax.InputSource;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXNotSupportedException;
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

    private int tail = 0;
    private SaxEventType[] events;
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
    private ContentHandler subContentHandler;
    private ErrorHandler errorHandler;
    private DTDHandler dtdHandler;

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

    private boolean modifiedSpace = false;
    private int oldSize;
    private int newSize;

    private void checkSpace(int stringSpace, int attsSpace, int intSpace, int charSpace) {
        modifiedSpace = false;
        if (tail >= events.length) {
            oldSize = events.length;
            newSize = oldSize * 2;
            SaxEventType[] tmpEvents = events;
            events = new SaxEventType[newSize];
            System.arraycopy(tmpEvents, 0, events, 0, oldSize);
            int[] tmpIndex = argIndex1;
            argIndex1 = new int[newSize];
            System.arraycopy(tmpIndex, 0, argIndex1, 0, oldSize);
            tmpIndex = argIndex2;
            argIndex2 = new int[newSize];
            System.arraycopy(tmpIndex, 0, argIndex2, 0, oldSize);
            modifiedSpace = true;
        }
        if (stringSpace > 0 && stringTail + stringSpace >= stringArgBuffer.length) {
            oldSize = stringArgBuffer.length;
            newSize = oldSize * 2;
            String[] tmpString = stringArgBuffer;
            stringArgBuffer = new String[newSize];
            System.arraycopy(tmpString, 0, stringArgBuffer, 0, oldSize);
            modifiedSpace = true;
        }
        if (attsSpace > 0 && attsTail + attsSpace >= attsArgBuffer.length) {
            oldSize = attsArgBuffer.length;
            newSize = oldSize * 2;
            Attributes[] tmpAtts = attsArgBuffer;
            attsArgBuffer = new Attributes[newSize];
            System.arraycopy(tmpAtts, 0, attsArgBuffer, 0, oldSize);
            modifiedSpace = true;
        }
        if (intSpace > 0 && intTail + intSpace >= intArgBuffer.length) {
            oldSize = intArgBuffer.length;
            newSize = oldSize * 2;
            int[] tmpInt = intArgBuffer;
            intArgBuffer = new int[newSize];
            System.arraycopy(tmpInt, 0, intArgBuffer, 0, oldSize);
            modifiedSpace = true;
        }
        if (charSpace > 0 && charTail + charSpace >= charArgBuffer.length) {
            oldSize = charArgBuffer.length;
            newSize = oldSize * 2;
            char[] tmpChar = charArgBuffer;
            charArgBuffer = new char[newSize];
            System.arraycopy(tmpChar, 0, charArgBuffer, 0, oldSize);
            modifiedSpace = true;
        }
        if (modifiedSpace) {
            checkSpace(stringSpace, attsSpace, intSpace, charSpace);
        }
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
        checkSpace(0, 0, 1, length);
        argIndex1[tail] = intTail;
        intArgBuffer[intTail++] = charTail;
        intArgBuffer[intTail++] = length;
        System.arraycopy(ch, start, charArgBuffer, charTail, length);
        charTail += length;
        events[tail++] = SaxEventType.ignorableWhitespace;
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

    /*
     * Execution
     */

    private boolean parsing = false;

    @Override
    public void parse(InputSource input) throws SAXException, IOException {
        parseIgnoringInput();
    }

    @Override
    public void parse(String systemId) throws SAXException, IOException {
        parseIgnoringInput();
    }

    private void parseIgnoringInput() throws SAXException {
        if (parsing) {
            throw new IllegalStateException();
        }
        parsing = true;
        for (int i = 0; i < tail; i++) {
            execute(i, getContentHandler());
        }
        parsing = false;
    }

    public void play(ContentHandler ch) throws SAXException {
        for (int i = 0; i < tail; i++) {
            execute(i, ch);
        }
    }

    public void dump(PrintStream out, boolean writeWhitespaceCharacterEvents) throws SAXException {
        for (int i = 0; i < tail; i++) {
            dump(i, out, writeWhitespaceCharacterEvents);
        }
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

    private void dump(int index, PrintStream out, boolean writeWhitespaceCharacterEvents) throws SAXException {
        switch (events[index]) {
            case startDocument:
                out.println("startDocument()");
                break;
            case endDocument:
                out.println("endDocument()");
                break;
            case startPrefixMapping:
                dumpStartPrefixMapping(index, out);
                break;
            case endPrefixMapping:
                dumpEndPrefixMapping(index, out);
                break;
            case startElement:
                dumpStartElement(index, out);
                break;
            case endElement:
                dumpEndElement(index, out);
                break;
            case characters:
                dumpCharacters(index, out, writeWhitespaceCharacterEvents);
                break;
            case ignorableWhitespace:
                if (writeWhitespaceCharacterEvents) {
                    dumpIgnorableWhitespace(index, out);
                }
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
        indType1 = argIndex1[index];
        ch.ignorableWhitespace(charArgBuffer, intArgBuffer[indType1], intArgBuffer[indType1 + 1]);
    }

    private void dumpStartPrefixMapping(int index, PrintStream out) throws SAXException {
        indType1 = argIndex1[index];
        out.println("startPrefixMapping("+stringArgBuffer[indType1]+", "+stringArgBuffer[indType1 + 1]+")");
    }

    private void dumpEndPrefixMapping(int index, PrintStream out) throws SAXException {
        indType1 = argIndex1[index];
        out.println("endPrefixMapping("+stringArgBuffer[indType1]+")");
    }

    private void dumpStartElement(int index, PrintStream out) throws SAXException {
        indType1 = argIndex1[index];
        indType2 = argIndex2[index];
        out.println("startElement("+stringArgBuffer[indType1]+", "+stringArgBuffer[indType1 + 1]+", "+stringArgBuffer[indType1 + 2]+", "+attsToString(attsArgBuffer[indType2])+")");
    }

    private void dumpEndElement(int index, PrintStream out) throws SAXException {
        indType1 = argIndex1[index];
        out.println("endElement("+stringArgBuffer[indType1]+", "+stringArgBuffer[indType1 + 1]+", "+stringArgBuffer[indType1 + 2]+")");
    }

    private void dumpCharacters(int index, PrintStream out, boolean writeWhitespaceCharacterEvents) throws SAXException {
        indType1 = argIndex1[index];
        String characterString = getPrintingCharacters(charArgBuffer, intArgBuffer[indType1], intArgBuffer[indType1 + 1], writeWhitespaceCharacterEvents);
        if (writeWhitespaceCharacterEvents || characterString != null) {
            out.println("characters("+characterString+")");
        }
    }

    private void dumpIgnorableWhitespace(int index, PrintStream out) throws SAXException {
        indType1 = argIndex1[index];
        String characterString = getPrintingCharacters(charArgBuffer, intArgBuffer[indType1], intArgBuffer[indType1 + 1], true);
        out.println("ignorableWhitespace(" + characterString + ")");
    }

    private String getPrintingCharacters(char[] ch, int start, int length, boolean writeWhitespaceCharacterEvents) {
        String s = new String(charArgBuffer, intArgBuffer[indType1], intArgBuffer[indType1 + 1]);
        if (!writeWhitespaceCharacterEvents && s.matches("^\\s*$")) {
            return null;
        } else {
            return s.replaceAll("\n", "\\\\n");
        }
    }

    private static String attsToString(Attributes atts) {
        StringBuilder sb = new StringBuilder();
        sb.append(atts.getClass().getSimpleName()).append('[');
        for (int i = 0; i < atts.getLength(); i++) {
            sb.append(atts.getURI(i)).append(atts.getQName(i)).append("=\"").append(atts.getValue(i));
            if (i < atts.getLength() - 1) {
                sb.append("\", ");
            } else {
                sb.append('"');
            }
        }
        sb.append(']');
        return sb.toString();
    }

    @Override
    public void setDocumentLocator(Locator locator) {
        logger.trace("ignoring setDocumentLocator(" + locator + ")");
    }

    @Override
    public boolean getFeature(String name) throws SAXNotRecognizedException, SAXNotSupportedException {
        if (name.equals("http://xml.org/sax/features/namespaces")) {
            return true;
        }
        throw new UnsupportedOperationException("getFeature("+name+")");
    }

    @Override
    public void setFeature(String name, boolean value) throws SAXNotRecognizedException, SAXNotSupportedException {
        if (name.equals("http://xml.org/sax/features/namespaces")) {
            if (!value) {
                throw new IllegalStateException("cannot set namespaces feature to false");
            }
        } else {
            logger.trace("ignoring setFeature(" + name + ", " + value + ")");
        }
    }

    @Override
    public Object getProperty(String name) throws SAXNotRecognizedException, SAXNotSupportedException {
        return null;
    }

    @Override
    public void setProperty(String name, Object value) throws SAXNotRecognizedException, SAXNotSupportedException {
        logger.trace("ignoring setProperty(" + name + ", " + value + ")");
    }

    @Override
    public void setEntityResolver(EntityResolver resolver) {
        throw new UnsupportedOperationException("Not supported.");
    }

    @Override
    public EntityResolver getEntityResolver() {
        throw new UnsupportedOperationException("Not supported.");
    }

    @Override
    public void setDTDHandler(DTDHandler handler) {
        dtdHandler = handler;
    }

    @Override
    public DTDHandler getDTDHandler() {
        return dtdHandler;
    }

    @Override
    public void setContentHandler(ContentHandler handler) {
        subContentHandler = handler;
    }

    @Override
    public ContentHandler getContentHandler() {
        return subContentHandler;
    }

    @Override
    public void setErrorHandler(ErrorHandler handler) {
        errorHandler = handler;
    }

    @Override
    public ErrorHandler getErrorHandler() {
        return errorHandler;
    }

}
