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

import org.xml.sax.SAXException;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXNotSupportedException;
import org.xml.sax.ext.LexicalHandler;
import org.xml.sax.helpers.XMLFilterImpl;

/**
 *
 * @author michael
 */
public class XMLFilterLexicalHandlerImpl extends VolatileXMLFilterImpl implements LexicalHandler {

    public static final String LEXICAL_HANDLER_PROPERTY_KEY = "http://xml.org/sax/properties/lexical-handler";

    protected LexicalHandler lh;

    @Override
    public void startDTD(String name, String publicId, String systemId) throws SAXException {
        if (lh != null) {
            lh.startDTD(name, publicId, systemId);
        }
    }

    @Override
    public void endDTD() throws SAXException {
        if (lh != null) {
            lh.endDTD();
        }
    }

    @Override
    public void startEntity(String name) throws SAXException {
        if (lh != null) {
            lh.startEntity(name);
        }
    }

    @Override
    public void endEntity(String name) throws SAXException {
        if (lh != null) {
            lh.endEntity(name);
        }
    }

    @Override
    public void startCDATA() throws SAXException {
        if (lh != null) {
            lh.startCDATA();
        }
    }

    @Override
    public void endCDATA() throws SAXException {
        if (lh != null) {
            lh.endCDATA();
        }
    }

    @Override
    public void comment(char[] ch, int start, int length) throws SAXException {
        if (lh != null) {
            lh.comment(ch, start, length);
        }
    }

    @Override
    public Object getProperty(String name) throws SAXNotRecognizedException, SAXNotSupportedException {
        if (name.equals(LEXICAL_HANDLER_PROPERTY_KEY)) {
            return lh;
        } else {
            return super.getProperty(name);
        }
    }

    @Override
    public void setProperty(String name, Object value) throws SAXNotRecognizedException, SAXNotSupportedException {
        if (name.equals(LEXICAL_HANDLER_PROPERTY_KEY)) {
            lh = (LexicalHandler) value;
        } else {
            super.setProperty(name, value);
        }
    }

}
