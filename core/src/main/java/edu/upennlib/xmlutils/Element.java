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

import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;

/**
 *
 * @author michael
 */
public class Element {

    public final String uri;
    public final String prefix;
    public final String localName;
    public final String qName;
    public static final Attributes EMPTY_ATTS = new UnmodifiableAttributes();

    public Element(String uri, String prefix, String localName) {
        this.uri = uri.intern();
        this.prefix = prefix.intern();
        this.localName = localName.intern();
        if (prefix.equals("")) {
            qName = this.localName;
        } else {
            qName = (prefix + ":" + localName).intern();
        }
    }

    public Element(String localName) {
        uri = "";
        prefix = "";
        qName = localName.intern();
        this.localName = localName = qName;
    }

    public void start(ContentHandler ch) throws SAXException {
        start(ch, EMPTY_ATTS);
    }

    public void start(ContentHandler ch, Attributes atts) throws SAXException {
        ch.startElement(uri, localName, qName, atts);
    }

    public void end(ContentHandler ch) throws SAXException {
        ch.endElement(uri, localName, qName);
    }

    public static void logXML(ContentHandler ch, Element element, String message) throws SAXException {
        char[] messageChars = message.toCharArray();
        element.start(ch);
        ch.characters(messageChars, 0, messageChars.length);
        element.end(ch);
    }

    private static class UnmodifiableAttributes extends AttributesImpl {

        @Override
        public void addAttribute(String uri, String localName, String qName, String type, String value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setAttribute(int index, String uri, String localName, String qName, String type, String value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setAttributes(Attributes atts) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setLocalName(int index, String localName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setQName(int index, String qName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setType(int index, String type) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setURI(int index, String uri) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setValue(int index, String value) {
            throw new UnsupportedOperationException();
        }

    }

}
