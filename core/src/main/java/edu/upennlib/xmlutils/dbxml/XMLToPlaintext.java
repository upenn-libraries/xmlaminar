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

package edu.upennlib.xmlutils.dbxml;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import javax.xml.parsers.SAXParserFactory;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLFilterImpl;

/**
 *
 * @author magibney
 */
public class XMLToPlaintext extends XMLFilterImpl implements Runnable {
    int level = 0;
    private final StringBuilder sb = new StringBuilder();
    private final Writer out;
    private final InputSource in;

    public XMLToPlaintext(Writer out, InputSource in) {
        this.out = out;
        this.in = in;
    }

    public XMLToPlaintext(Writer out, InputSource in, XMLReader parent) {
        super(parent);
        this.out = out;
        this.in = in;
    }

    public static void main(String[] args) throws Exception {
        Writer w = new OutputStreamWriter(System.out);
        InputSource in = new InputSource("../cli/work/tmp.xml");
        XMLToPlaintext x = new XMLToPlaintext(w, in, SAXParserFactory.newInstance().newSAXParser().getXMLReader());
        x.run();
    }

    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        sb.append(ch, start, length);
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        level--;
        handoff();
    }

    private void handoff() {
        XMLReader parent = getParent();
        try {
            out.append(sb);
            out.write(LS);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        XMLToPlaintextStatic replacement = new XMLToPlaintextStatic(level + 1, out, level, parent);
        parent.setContentHandler(replacement);
    }

    @Override
    public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
        sb.setLength(0);
        level++;
    }

    @Override
    public void setContentHandler(ContentHandler handler) {
        throw new UnsupportedOperationException("not supported");
    }
    
    private static final char[] LS = System.lineSeparator().toCharArray();

    @Override
    public void run() {
        try {
            XMLReader parent = getParent();
            parent.setContentHandler(this);
            parent.parse(in);
            out.close();
        } catch (SAXException ex) {
            ex.printStackTrace(System.err);
            throw new RuntimeException(ex);
        } catch (IOException ex) {
            ex.printStackTrace(System.err);
            throw new RuntimeException(ex);
        } catch (RuntimeException ex) {
            ex.printStackTrace(System.err);
            throw ex;
        } catch (Error e) {
            e.printStackTrace(System.err);
            throw e;
        }
    }

    private static class XMLToPlaintextStatic extends XMLFilterImpl {

        private boolean write = false;
        private int level;
        
        private final int levelOfInterest;
        private final Writer out;

        public XMLToPlaintextStatic(int levelOfInterest, Writer out, int level) {
            this.levelOfInterest = levelOfInterest;
            this.out = out;
            this.level = level;
        }

        public XMLToPlaintextStatic(int levelOfInterest, Writer out, int level, XMLReader parent) {
            super(parent);
            this.levelOfInterest = levelOfInterest;
            this.out = out;
            this.level = level;
        }
        
        @Override
        public void characters(char[] ch, int start, int length) throws SAXException {
            if (write && level == levelOfInterest) {
                try {
                    out.write(ch, start, length);
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            }
        }

        @Override
        public void endElement(String uri, String localName, String qName) throws SAXException {
            if (level-- == levelOfInterest) {
                if (write) {
                    try {
                        out.write(LS);
                    } catch (IOException ex) {
                        throw new RuntimeException(ex);
                    }
                    write = false;
                }
            } else if (level == 1) {
                write = true;
            }
        }

        @Override
        public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
            level++;
        }
        
    }
    
}
