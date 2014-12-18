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
import org.xml.sax.Locator;
import org.xml.sax.SAXException;

/**
 *
 * @author Michael Gibney
 */
public class EchoingContentHandler implements ContentHandler {

    @Override
    public void setDocumentLocator(Locator locator) {

    }

    @Override
    public void startDocument() throws SAXException {
        System.out.println("startDocument()");
    }

    @Override
    public void endDocument() throws SAXException {
        System.out.println("endDocument()");
    }

    @Override
    public void startPrefixMapping(String prefix, String uri) throws SAXException {
        System.out.println("startPrefixMapping("+prefix+", "+uri+")");
    }

    @Override
    public void endPrefixMapping(String prefix) throws SAXException {
        System.out.println("endPrefixMapping("+prefix+")");
    }

    @Override
    public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
        System.out.println("startElement("+uri+", "+localName+", "+qName+")");
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        System.out.println("endElement("+uri+", "+localName+", "+qName+")");
    }

    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        //System.out.println("characters("+new String(ch, start, length)+")");
    }

    @Override
    public void ignorableWhitespace(char[] ch, int start, int length) throws SAXException {
        //System.out.println("ignorableWhitespace("+new String(ch, start, length)+")");
    }

    @Override
    public void processingInstruction(String target, String data) throws SAXException {
        System.out.println("processingInstruction("+target+", "+data+")");
    }

    @Override
    public void skippedEntity(String name) throws SAXException {
        System.out.println("skippedEntity("+name+")");
    }

}
