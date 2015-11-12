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

package edu.upenn.library.xmlaminar.parallel;

import java.io.IOException;
import org.xml.sax.ContentHandler;
import org.xml.sax.DTDHandler;
import org.xml.sax.EntityResolver;
import org.xml.sax.ErrorHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXNotSupportedException;
import org.xml.sax.XMLReader;

/**
 *
 * @author magibney
 */
public class DummyXMLReader implements XMLReader {

    @Override
    public boolean getFeature(String name) throws SAXNotRecognizedException, SAXNotSupportedException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setFeature(String name, boolean value) throws SAXNotRecognizedException, SAXNotSupportedException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Object getProperty(String name) throws SAXNotRecognizedException, SAXNotSupportedException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setProperty(String name, Object value) throws SAXNotRecognizedException, SAXNotSupportedException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setEntityResolver(EntityResolver resolver) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public EntityResolver getEntityResolver() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setDTDHandler(DTDHandler handler) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public DTDHandler getDTDHandler() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setContentHandler(ContentHandler handler) {
        // return immediately
    }

    @Override
    public ContentHandler getContentHandler() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setErrorHandler(ErrorHandler handler) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public ErrorHandler getErrorHandler() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void parse(InputSource input) throws IOException, SAXException {
        // return immediately
    }

    @Override
    public void parse(String systemId) throws IOException, SAXException {
        // return immediately
    }
    
}
