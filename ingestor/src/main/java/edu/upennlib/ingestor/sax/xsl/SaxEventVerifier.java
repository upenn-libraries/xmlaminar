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
import java.util.Iterator;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;

/**
 *
 * @author michael
 */
public class SaxEventVerifier implements ContentHandler {

    BufferingXMLFilter base = new BufferingXMLFilter();
    Iterator baseIterator;

    boolean recording = false;
    boolean verifying = false;

    public void recordStart() {
        recording = true;
        verifying = false;
    }

    public void recordEnd() {
        recording = false;
    }

    public void verifyStart() {
        recording = false;
        verifying = true;
        baseIterator = base.iterator();
    }

    public void verifyEnd() {
        if (verifying) {
            verifying = false;
            if (baseIterator.hasNext()) {
                throw new IllegalStateException("required base events not found, starting with: " + Arrays.asList((Object[])baseIterator.next()));
            }
        }
    }

    private void verify(Object... event) {
        if (!baseIterator.hasNext()) {
            throw new IllegalStateException("no corresponding base event for: " + Arrays.asList(event));
        } else {
            Object[] nextBase = (Object[]) baseIterator.next();
            if (!SaxEventExecutor.equals(nextBase, event)) {
                throw new IllegalStateException(Arrays.asList(nextBase) + "!=" + Arrays.asList(event));
            }
        }
    }

    @Override
    public void startDocument() throws SAXException {
        if (verifying) {
            verify(SaxEventType.startDocument);
        } else if (recording) {
            base.startDocument();
        }
    }

    @Override
    public void startPrefixMapping(String prefix, String uri) throws SAXException {
        if (verifying) {
            verify(SaxEventType.startPrefixMapping, prefix, uri);
        } else if (recording) {
            base.startPrefixMapping(prefix, uri);
        }
    }

    @Override
    public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
        if (verifying) {
            verify(SaxEventType.startElement, uri, localName, qName, atts);
        } else if (recording) {
            base.startElement(uri, localName, qName, atts);
        }
    }

    @Override
    public void endDocument() throws SAXException {
        if (verifying) {
            verify(SaxEventType.endDocument);
        } else if (recording) {
            base.endDocument();
        }
    }

    @Override
    public void endPrefixMapping(String prefix) throws SAXException {
        if (verifying) {
            verify(SaxEventType.endPrefixMapping, prefix);
        } else if (recording) {
            base.endPrefixMapping(prefix);
        }
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        if (verifying) {
            verify(SaxEventType.endElement, uri, localName, qName);
        } else if (recording) {
            base.endElement(uri, localName, qName);
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

}
