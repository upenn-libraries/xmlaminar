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

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package edu.upennlib.ingestor.sax.xsl;

import java.util.Arrays;
import java.util.LinkedList;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;

/**
 *
 * @author michael
 */
public class MyEndEventStack implements ContentHandler {

    private LinkedList<Object[]> endEventStack = new LinkedList<Object[]>();
    private boolean locked = false;

    public int writeEndEvents(ContentHandler ch) throws SAXException {
        int level = 0;
        for (Object[] endEvent : endEventStack) {
            switch ((SaxEventType)endEvent[0]) {
                case endElement:
                    ch.endElement((String)endEvent[1], (String)endEvent[2], (String)endEvent[3]);
                    level--;
                    break;
                case endPrefixMapping:
                    ch.endPrefixMapping((String)endEvent[1]);
                    break;
                case endDocument:
                    ch.endDocument();
                    break;
                default:
                    throw new RuntimeException();
            }
        }
        return level;
    }

    public void lock() {
        locked = true;
    }

    public void clear() {
        endEventStack.clear();
        locked = false;
    }

    private void push(Object... args) {
        endEventStack.addFirst(args);
    }

    private void pop(Object... args) {
        Object[] remove = endEventStack.remove();
        for (int i = 0; i < remove.length; i++) {
            if (!remove[i].equals(args[i])) {
                throw new IllegalStateException(Arrays.asList(args) + " !=  " + Arrays.asList(remove));
            }
        }
    }

    @Override
    public void startDocument() throws SAXException {
        if (!locked) {
            push(SaxEventType.endDocument);
        }
    }

    @Override
    public void startPrefixMapping(String prefix, String uri) throws SAXException {
        if (!locked) {
            push(SaxEventType.endPrefixMapping, prefix);
        }
    }

    @Override
    public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
        if (!locked) {
            push(SaxEventType.endElement, uri, localName, qName);
        }
    }

    @Override
    public void endDocument() throws SAXException {
        if (!locked) {
            pop(SaxEventType.endDocument);
        }
    }

    @Override
    public void endPrefixMapping(String prefix) throws SAXException {
        if (!locked) {
            pop(SaxEventType.endPrefixMapping, prefix);
        }
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        if (!locked) {
            pop(SaxEventType.endElement, uri, localName, qName);
        }
    }

    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
    }

    @Override
    public void ignorableWhitespace(char[] ch, int start, int length) throws SAXException {
    }

    @Override
    public void processingInstruction(String target, String data) throws SAXException {
    }

    @Override
    public void skippedEntity(String name) throws SAXException {
    }

    @Override
    public void setDocumentLocator(Locator locator) {
    }

    void startEntity(String name) {
        if (!locked) {
            push(SaxEventType.endEntity, name);
        }
    }

    void startDTD(String name, String publicId, String systemId) {
        if (!locked) {
            push(SaxEventType.endDTD);
        }
    }

    void startCDATA() {
        if (!locked) {
            push(SaxEventType.endCDATA);
        }
    }

    void endEntity(String name) {
        if (!locked) {
            pop(SaxEventType.endEntity, name);
        }
    }

    void endDTD() {
        if (!locked) {
            pop(SaxEventType.endDTD);
        }
    }

    void endCDATA() {
        if (!locked) {
            pop(SaxEventType.endCDATA);
        }
    }

}
