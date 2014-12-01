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

import java.io.IOException;
import java.io.PrintStream;
import org.apache.log4j.Logger;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXNotSupportedException;
import org.xml.sax.XMLReader;
import org.xml.sax.ext.LexicalHandler;
import org.xml.sax.helpers.AttributesImpl;

/**
 *
 * @author michael
 */
public class UnboundedContentHandlerBuffer extends XMLFilterLexicalHandlerImpl {

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

    private AttributesImpl[] attsArgBuffer;
    private int attsTail = 0;

    private int[] intArgBuffer;
    private int intTail = 0;

    private char[] charArgBuffer;
    private int charTail = 0;

    private Logger logger = Logger.getLogger(getClass());
    
    private volatile XMLReader unmodifiableParent;

    private boolean parentModifiable = true;

    public void setParentModifiable(boolean modifiable) {
        this.parentModifiable = modifiable;
    }

    public XMLReader getUnmodifiableParent() {
        return unmodifiableParent;
    }
    
    public void setUnmodifiableParent(XMLReader parent) {
        unmodifiableParent = parent;
        parentModifiable = false;
    }

    @Override
    public void setParent(XMLReader parent) {
        if (unmodifiableParent == null || unmodifiableParent == getParent()) {
            unmodifiableParent = parent;
        }
        super.setParent(parent);
        parentModifiable = true;
    }
    
    @Override
    public boolean getFeature(String name) throws SAXNotRecognizedException, SAXNotSupportedException {
        if (!parentModifiable) {
            return unmodifiableParent.getFeature(name);
        } else {
            return super.getFeature(name);
        }
    }

    @Override
    public void setFeature(String name, boolean value) throws SAXNotRecognizedException, SAXNotSupportedException {
        if (!parentModifiable) {
            if (unmodifiableParent.getFeature(name) != value) {
                throw new SAXNotSupportedException(unmodifiableParent + " is unmodifiable, and does not support setting feature "+name+" to "+value+".");
            }
        } else {
            super.setFeature(name, value);
        }
    }

    public UnboundedContentHandlerBuffer() {
        this(INITIAL_BUFFER_SIZE);
    }
    
    public UnboundedContentHandlerBuffer(int initialBufferSize) {
        events = new SaxEventType[initialBufferSize];
        argIndex1 = new int[initialBufferSize];
        argIndex2 = new int[initialBufferSize];
        stringArgBuffer = new String[initialBufferSize * STRING_ARGS_INIT_FACTOR];
        attsArgBuffer = new AttributesImpl[initialBufferSize];
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
            attsArgBuffer = new AttributesImpl[newSize];
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
        checkSpace(1, 0, 0, 0);
        argIndex1[tail] = stringTail;
        stringArgBuffer[stringTail++] = name;
        events[tail++] = SaxEventType.startEntity;
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

    private boolean stringIntern = false;

    private void parseIgnoringInput() throws SAXException {
        if (parsing) {
            throw new IllegalStateException();
        }
        parsing = true;
        stringIntern = getFeature(SAXFeatures.STRING_INTERNING);
        ContentHandler ch = getContentHandler();
        LexicalHandler lex = (LexicalHandler)getProperty(LEXICAL_HANDLER_PROPERTY_KEY);
        for (int i = 0; i < tail; i++) {
            execute(i, ch, lex);
        }
        if (flushOnParse) {
            clear();
        }
        parsing = false;
    }

    private boolean flushOnParse = false;
    
    public boolean isFlushOnParse() {
        return flushOnParse;
    }
    
    public void setFlushOnParse(boolean flush) {
        this.flushOnParse = flush;
    }
    
    public int play(ContentHandler ch, LexicalHandler lh) throws SAXException {
        int level = 0;
        for (int i = 0; i < tail; i++) {
                level += execute(i, ch, lh);
        }
        return level;
    }

    public int flush(ContentHandler ch, LexicalHandler lh) throws SAXException {
        int level = play(ch, lh);
        clear();
        return level;
    }

    public void clear() {
        tail = 0;
        stringTail = 0;
        attsTail = 0;
        intTail = 0;
        charTail = 0;
        parsing = false;
    }

    public void dump(PrintStream out, boolean writeWhitespaceCharacterEvents) throws SAXException {
        for (int i = 0; i < tail; i++) {
            dump(i, out, writeWhitespaceCharacterEvents);
        }
    }

    private boolean isSignificant(SaxEventType type) {
        switch (type) {
            case startDocument:
            case endDocument:
            case startPrefixMapping:
            case endPrefixMapping:
            case startElement:
            case endElement:
                return true;
            default:
                return false;
        }
    }

    public int size() {
        return tail;
    }

    public int playMostRecentStructurallyInsignificant(ContentHandler ch, LexicalHandler lh) throws SAXException {
        int level = 0;
        int i;
        for (i = tail - 1; i > -1; i--) {
            if (isSignificant(events[i])) {
                break;
            }
        }
        while (++i < tail) {
            level += execute(i, ch, lh);
        }
        return level;
    }


    //XXX intern?
    private boolean equalsIntern(UnboundedContentHandlerBuffer other) {
        for (int i = 0; i < stringTail; i++) {
            if (stringArgBuffer[i] != other.stringArgBuffer[i]) {
                return false;
            }
        }
        for (int i = 0; i < attsTail; i++) {
            if (!testAttributeEqualityIntern(attsArgBuffer[i], other.attsArgBuffer[i])) {
                return false;
            }
        }
        return true;
    }

    private boolean equalsNoIntern(UnboundedContentHandlerBuffer other) {
        for (int i = 0; i < stringTail; i++) {
            if (!stringArgBuffer[i].equals(other.stringArgBuffer[i])) {
                return false;
            }
        }
        for (int i = 0; i < attsTail; i++) {
            if (!testAttributeEqualityNoIntern(attsArgBuffer[i], other.attsArgBuffer[i])) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof UnboundedContentHandlerBuffer)) {
            return false;
        }
        UnboundedContentHandlerBuffer other = (UnboundedContentHandlerBuffer) o;
        if (other.size() != size()) {
            return false;
        }
        for (int i = 0; i < tail; i++) {
            if (other.events[i] != events[i]) {
                return false;
            }
            if (other.argIndex1[i] != argIndex1[i]) {
                return false;
            }
            if (other.argIndex2[i] != argIndex2[i]) {
                return false;
            }
        }
        if (stringIntern) {
            if (!equalsIntern(other)) {
                return false;
            }
        } else {
            if (!equalsNoIntern(other)) {
                return false;
            }
        }
        for (int i = 0; i < intTail; i++) {
            if (intArgBuffer[i] != other.intArgBuffer[i]) {
                return false;
            }
        }
        for (int i = 0; i < charTail; i++) {
            if (charArgBuffer[i] != other.charArgBuffer[i]) {
                return false;
            }
        }
        return true;
    }

    //XXX intern?
    private static boolean testAttributeEqualityIntern(Attributes attOne, Attributes attTwo) {
        if (attOne.getLength() != attTwo.getLength()) {
            return false;
        }
        for (int i = 0; i < attOne.getLength(); i++) {
            if (attOne.getURI(i) != attTwo.getURI(i)) {
                return false;
            }
            if (attOne.getLocalName(i) != attTwo.getLocalName(i)) {
                return false;
            }
            if (attOne.getQName(i) != attTwo.getQName(i)) {
                return false;
            }
            if (!attOne.getType(i).equals(attTwo.getType(i))) {
                return false;
            }
            if (!attOne.getValue(i).equals(attTwo.getValue(i))) {
                return false;
            }
        }
        return true;
    }

    private static boolean testAttributeEqualityNoIntern(Attributes attOne, Attributes attTwo) {
        if (attOne.getLength() != attTwo.getLength()) {
            return false;
        }
        for (int i = 0; i < attOne.getLength(); i++) {
            if (!attOne.getURI(i).equals(attTwo.getURI(i))) {
                return false;
            }
            if (!attOne.getLocalName(i).equals(attTwo.getLocalName(i))) {
                return false;
            }
            if (!attOne.getQName(i).equals(attTwo.getQName(i))) {
                return false;
            }
            if (!attOne.getType(i).equals(attTwo.getType(i))) {
                return false;
            }
            if (!attOne.getValue(i).equals(attTwo.getValue(i))) {
                return false;
            }
        }
        return true;
    }

    public boolean verifyStartDocument(int index) {
        return tail > index && events[index] == SaxEventType.startDocument;
    }

    public boolean verifyEndDocument(int index) {
        return tail > index && events[index] == SaxEventType.endDocument;
    }

    public boolean verifyStartElement(int index, String uri, String localName, String qName, Attributes atts) {
        if (tail <= index) {
            return false;
        }
        if (events[index] != SaxEventType.startElement) {
            return false;
        }
        int strInd = argIndex1[index];
        int attsInd = argIndex2[index];
        if (stringIntern) {
            if (uri != stringArgBuffer[strInd]) {
                return false;
            }
            if (localName != stringArgBuffer[strInd + 1]) {
                return false;
            }
            if (qName != stringArgBuffer[strInd + 2]) {
                return false;
            }
            return testAttributeEqualityIntern(atts, attsArgBuffer[attsInd]);
        } else {
            if (!uri.equals(stringArgBuffer[strInd])) {
                return false;
            }
            if (!localName.equals(stringArgBuffer[strInd + 1])) {
                return false;
            }
            if (!qName.equals(stringArgBuffer[strInd + 2])) {
                return false;
            }
            return testAttributeEqualityNoIntern(atts, attsArgBuffer[attsInd]);
        }
    }

    public boolean verifyEndElement(int index, String uri, String localName, String qName) {
        if (tail <= index) {
            return false;
        }
        if (events[index] != SaxEventType.endElement) {
            return false;
        }
        int strInd = argIndex1[index];
        if (stringIntern) {
            if (uri != stringArgBuffer[strInd]) {
                return false;
            }
            if (localName != stringArgBuffer[strInd + 1]) {
                return false;
            }
            return qName == stringArgBuffer[strInd + 2];
        } else {
            if (!uri.equals(stringArgBuffer[strInd])) {
                return false;
            }
            if (!localName.equals(stringArgBuffer[strInd + 1])) {
                return false;
            }
            return qName.equals(stringArgBuffer[strInd + 2]);
        }
    }

    public boolean verifyStartPrefixMapping(int index, String prefix, String uri) {
        if (tail <= index) {
            return false;
        }
        if (events[index] != SaxEventType.startPrefixMapping) {
            return false;
        }
        int strInd = argIndex1[index];
        if (stringIntern) {
            if (prefix != stringArgBuffer[strInd]) {
                return false;
            }
            return uri == stringArgBuffer[strInd + 1];
        } else {
            if (!prefix.equals(stringArgBuffer[strInd])) {
                return false;
            }
            return uri.equals(stringArgBuffer[strInd + 1]);
        }
    }

    public boolean verifyEndPrefixMapping(int index, String prefix) {
        if (tail <= index) {
            return false;
        }
        if (events[index] != SaxEventType.endPrefixMapping) {
            return false;
        }
        int strInd = argIndex1[index];
        if (stringIntern) {
            return prefix == stringArgBuffer[strInd];
        } else {
            return prefix.equals(stringArgBuffer[strInd]);
        }
    }

    private int execute(int index, ContentHandler ch, LexicalHandler lh) throws SAXException {
        switch (events[index]) {
            case startDocument:
                ch.startDocument();
                return 0;
            case endDocument:
                ch.endDocument();
                return 0;
            case startPrefixMapping:
                indexType1 = argIndex1[index];
                ch.startPrefixMapping(stringArgBuffer[indexType1], stringArgBuffer[indexType1 + 1]);
                return 0;
            case endPrefixMapping:
                indexType1 = argIndex1[index];
                ch.endPrefixMapping(stringArgBuffer[indexType1]);
                return 0;
            case startElement:
                indexType1 = argIndex1[index];
                indexType2 = argIndex2[index];
                ch.startElement(stringArgBuffer[indexType1], stringArgBuffer[indexType1 + 1], stringArgBuffer[indexType1 + 2], attsArgBuffer[indexType2]);
                return 1;
            case endElement:
                indexType1 = argIndex1[index];
                ch.endElement(stringArgBuffer[indexType1], stringArgBuffer[indexType1 + 1], stringArgBuffer[indexType1 + 2]);
                return -1;
            case characters:
                indexType1 = argIndex1[index];
                ch.characters(charArgBuffer, intArgBuffer[indexType1], intArgBuffer[indexType1 + 1]);
                return 0;
            case ignorableWhitespace:
                indexType1 = argIndex1[index];
                ch.ignorableWhitespace(charArgBuffer, intArgBuffer[indexType1], intArgBuffer[indexType1 + 1]);
                return 0;
            case startDTD:
                indexType1 = argIndex1[index];
                lh.startDTD(stringArgBuffer[indexType1], stringArgBuffer[indexType1 + 1], stringArgBuffer[indexType1 + 2]);
                return 0;
            case endDTD:
                lh.endDTD();
                return 0;
            case startEntity:
                indexType1 = argIndex1[index];
                lh.startEntity(stringArgBuffer[indexType1]);
                return 0;
            case endEntity:
                indexType1 = argIndex1[index];
                lh.endEntity(stringArgBuffer[indexType1]);
                return 0;
            case startCDATA:
                lh.startCDATA();
                return 0;
            case endCDATA:
                lh.endCDATA();
                return 0;
            case comment:
                indexType1 = argIndex1[index];
                lh.comment(charArgBuffer, intArgBuffer[indexType1], intArgBuffer[indexType1 + 1]);
                return 0;
            default:
                return 0;
        }
    }

    private void dump(int index, PrintStream out, boolean writeWhitespaceCharacterEvents) throws SAXException {
        String characterString;
        switch (events[index]) {
            case startDocument:
                out.println("startDocument()");
                break;
            case endDocument:
                out.println("endDocument()");
                break;
            case startPrefixMapping:
                indexType1 = argIndex1[index];
                out.println("startPrefixMapping(" + stringArgBuffer[indexType1] + ", " + stringArgBuffer[indexType1 + 1] + ")");
                break;
            case endPrefixMapping:
                indexType1 = argIndex1[index];
                out.println("endPrefixMapping(" + stringArgBuffer[indexType1] + ")");
                break;
            case startElement:
                indexType1 = argIndex1[index];
                indexType2 = argIndex2[index];
                out.println("startElement(" + stringArgBuffer[indexType1] + ", " + stringArgBuffer[indexType1 + 1] + ", " + stringArgBuffer[indexType1 + 2] + ", " + attsToString(attsArgBuffer[indexType2]) + ")");
                break;
            case endElement:
                indexType1 = argIndex1[index];
                out.println("endElement(" + stringArgBuffer[indexType1] + ", " + stringArgBuffer[indexType1 + 1] + ", " + stringArgBuffer[indexType1 + 2] + ")");
                break;
            case characters:
                indexType1 = argIndex1[index];
                characterString = getPrintingCharacters(charArgBuffer, intArgBuffer[indexType1], intArgBuffer[indexType1 + 1], writeWhitespaceCharacterEvents);
                if (writeWhitespaceCharacterEvents || characterString != null) {
                    out.println("characters(" + characterString + ")");
                }
                break;
            case ignorableWhitespace:
                if (writeWhitespaceCharacterEvents) {
                    indexType1 = argIndex1[index];
                    characterString = getPrintingCharacters(charArgBuffer, intArgBuffer[indexType1], intArgBuffer[indexType1 + 1], true);
                    out.println("ignorableWhitespace(" + characterString + ")");
                }
                break;
            case startDTD:
                indexType1 = argIndex1[index];
                out.println("startDTD(" + stringArgBuffer[indexType1] + ", " + stringArgBuffer[indexType1 + 1] + ", " + stringArgBuffer[indexType1 + 2] + ")");
                break;
            case endDTD:
                out.println("endDTD()");
                break;
            case startEntity:
                indexType1 = argIndex1[index];
                out.println("startEntity(" + stringArgBuffer[indexType1] + ")");
                break;
            case endEntity:
                indexType1 = argIndex1[index];
                out.println("endEntity(" + stringArgBuffer[indexType1] + ")");
                break;
            case startCDATA:
                out.println("startCDATA()");
                break;
            case endCDATA:
                out.println("endCDATA()");
                break;
            case comment:
                indexType1 = argIndex1[index];
                characterString = getPrintingCharacters(charArgBuffer, intArgBuffer[indexType1], intArgBuffer[indexType1 + 1], writeWhitespaceCharacterEvents);
                if (writeWhitespaceCharacterEvents || characterString != null) {
                    out.println("comment(" + characterString + ")");
                }
                break;
        }
    }

    private int indexType1;
    private int indexType2;

    private String getPrintingCharacters(char[] ch, int start, int length, boolean writeWhitespaceCharacterEvents) {
        String s = new String(ch, start, length);
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
        if (logger.isTraceEnabled()) {
            logger.trace("ignoring setDocumentLocator(" + locator + ")");
        }
    }

    public void writeWithFinalElementSelfAttribute(ContentHandler ch, LexicalHandler lh, boolean asSelf) throws SAXException {
        for (int i = 0; i < tail; i++) {
            if (i == tail - 1 && asSelf) {
                if (events[i] != SaxEventType.startElement) {
                    throw new IllegalStateException("called method at bad time.");
                }
                int strInd = argIndex1[i];
                AttributesImpl atts = attsArgBuffer[argIndex2[i]];
                atts.addAttribute("", "self", "self", "CDATA", "true");
                ch.startElement(stringArgBuffer[strInd], stringArgBuffer[strInd + 1], stringArgBuffer[strInd + 2], atts);
            } else {
                execute(i, ch, lh);
            }
        }
    }

    @Override
    public void startDTD(String name, String publicId, String systemId) throws SAXException {
        checkSpace(3, 0, 0, 0);
        argIndex1[tail] = stringTail;
        stringArgBuffer[stringTail++] = name;
        stringArgBuffer[stringTail++] = publicId;
        stringArgBuffer[stringTail++] = systemId;
        events[tail++] = SaxEventType.startDTD;
    }

    @Override
    public void endDTD() throws SAXException {
        checkSpace(0, 0, 0, 0);
        events[tail++] = SaxEventType.endDTD;
    }

    @Override
    public void startEntity(String name) throws SAXException {
        checkSpace(1, 0, 0, 0);
        argIndex1[tail] = stringTail;
        stringArgBuffer[stringTail++] = name;
        events[tail++] = SaxEventType.startEntity;
    }

    @Override
    public void endEntity(String name) throws SAXException {
        checkSpace(1, 0, 0, 0);
        argIndex1[tail] = stringTail;
        stringArgBuffer[stringTail++] = name;
        events[tail++] = SaxEventType.endEntity;
    }

    @Override
    public void startCDATA() throws SAXException {
        checkSpace(0, 0, 0, 0);
        events[tail++] = SaxEventType.startCDATA;
    }

    @Override
    public void endCDATA() throws SAXException {
        checkSpace(0, 0, 0, 0);
        events[tail++] = SaxEventType.endCDATA;
    }

    @Override
    public void comment(char[] ch, int start, int length) throws SAXException {
        checkSpace(0, 0, 1, length);
        argIndex1[tail] = intTail;
        intArgBuffer[intTail++] = charTail;
        intArgBuffer[intTail++] = length;
        System.arraycopy(ch, start, charArgBuffer, charTail, length);
        charTail += length;
        events[tail++] = SaxEventType.comment;
    }

}
