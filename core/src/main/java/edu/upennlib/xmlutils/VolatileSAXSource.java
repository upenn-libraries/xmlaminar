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

import javax.xml.transform.sax.SAXSource;

import org.xml.sax.InputSource;
import org.xml.sax.XMLReader;

/**
 * A trivial reimplementation/drop-in replacement 
 * for javax.xml.transform.sax.SAXSource that makes instance variables
 * volatile (and thus appropriate for use by multiple threads)
 *
 * @author magibney
 */
public class VolatileSAXSource extends SAXSource {

    private volatile XMLReader reader;
    private volatile InputSource inputSource;

    public VolatileSAXSource() {
    }

    public VolatileSAXSource(XMLReader reader, InputSource inputSource) {
        this.reader = reader;
        this.inputSource = inputSource;
    }

    public VolatileSAXSource(InputSource inputSource) {
        this(null, inputSource);
    }

    @Override
    public void setXMLReader(XMLReader reader) {
        this.reader = reader;
    }

    @Override
    public XMLReader getXMLReader() {
        return reader;
    }

    @Override
    public void setInputSource(InputSource inputSource) {
        this.inputSource = inputSource;
    }

    @Override
    public InputSource getInputSource() {
        return inputSource;
    }

    @Override
    public void setSystemId(String systemId) {
        if (inputSource == null) {
            inputSource = new InputSource(systemId);
        } else {
            inputSource.setSystemId(systemId);
        }
    }

    @Override
    public String getSystemId() {
        return inputSource == null ? null : inputSource.getSystemId();
    }

}
