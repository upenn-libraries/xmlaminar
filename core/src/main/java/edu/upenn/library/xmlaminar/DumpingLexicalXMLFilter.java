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

package edu.upenn.library.xmlaminar;

import org.xml.sax.SAXException;
import org.xml.sax.ext.LexicalHandler;

/**
 *
 * @author michael
 */
public class DumpingLexicalXMLFilter extends DumpingXMLFilter implements LexicalHandler {

    @Override
    public void startDTD(String name, String publicId, String systemId) throws SAXException {
        ((LexicalHandler)getContentHandler()).startDTD(name, publicId, systemId);
        dfHandler.startDTD(name, publicId, systemId);
    }

    @Override
    public void endDTD() throws SAXException {
        ((LexicalHandler)getContentHandler()).endDTD();
        dfHandler.endDTD();
    }

    @Override
    public void startEntity(String name) throws SAXException {
        ((LexicalHandler)getContentHandler()).startEntity(name);
        dfHandler.startEntity(name);
    }

    @Override
    public void endEntity(String name) throws SAXException {
        ((LexicalHandler)getContentHandler()).endEntity(name);
        dfHandler.endEntity(name);
    }

    @Override
    public void startCDATA() throws SAXException {
        ((LexicalHandler)getContentHandler()).startCDATA();
        dfHandler.startCDATA();
    }

    @Override
    public void endCDATA() throws SAXException {
        ((LexicalHandler)getContentHandler()).endCDATA();
        dfHandler.endCDATA();
    }

    @Override
    public void comment(char[] ch, int start, int length) throws SAXException {
        ((LexicalHandler)getContentHandler()).comment(ch, start, length);
        dfHandler.comment(ch, start, length);
    }

}
