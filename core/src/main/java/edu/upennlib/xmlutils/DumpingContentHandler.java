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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.sax.SAXTransformerFactory;
import javax.xml.transform.sax.TransformerHandler;
import javax.xml.transform.stream.StreamResult;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.ext.LexicalHandler;
import org.xml.sax.helpers.XMLFilterImpl;

/**
 *
 * @author michael
 */
public class DumpingContentHandler extends XMLFilterImpl implements LexicalHandler {

    private boolean dump = false;
    private File df;
    private TransformerHandler dfHandler;
    private OutputStream dfOut;
    public static final String TRANSFORMER_FACTORY_CLASS = "net.sf.saxon.TransformerFactoryImpl";

    public static void main(String[] args) throws TransformerConfigurationException, ParserConfigurationException, SAXException, TransformerException {
        TransformerFactory tf = TransformerFactory.newInstance(TRANSFORMER_FACTORY_CLASS, null);
        Transformer t = tf.newTransformer();
        DumpingContentHandler instance = new DumpingContentHandler();
        instance.setDumpFile(new File("/tmp/video_dump.xml"));
        SAXParserFactory spf = SAXParserFactory.newInstance();
        spf.setNamespaceAware(true);
        instance.setParent(spf.newSAXParser().getXMLReader());
        t.setOutputProperty(OutputKeys.INDENT, "yes");
        t.transform(new SAXSource(instance, new InputSource("/tmp/video.xml")), new StreamResult("/tmp/video_echo.xml"));
    }

    public File getDumpFile() {
        return df;
    }

    public void setDumpFile(File dumpFile) {
        dump = dumpFile != null;
        this.df = dumpFile;
    }
    
    public void setDumpStream(OutputStream dumpOut) {
        dump = dumpOut != null;
        this.dfOut = dumpOut;
    }

    public OutputStream getDumpStream() {
        return dfOut;
    }
    
    private void initDump() {
        SAXTransformerFactory stf = (SAXTransformerFactory) TransformerFactory.newInstance(TRANSFORMER_FACTORY_CLASS, null);
        try {
            dfHandler = stf.newTransformerHandler();
            dfHandler.getTransformer().setOutputProperty(OutputKeys.INDENT, "yes");
            if (dfOut == null && df != null) {
                if (df.getName().endsWith(".gz")) {
                    dfOut = new GZIPOutputStream(new FileOutputStream(df));
                } else {
                    dfOut = new BufferedOutputStream(new FileOutputStream(df));
                }
            }
            dfHandler.setResult(new StreamResult(dfOut));
        } catch (TransformerConfigurationException ex) {
            throw new RuntimeException(ex);
        } catch (FileNotFoundException ex) {
            throw new RuntimeException(ex);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void parse(InputSource input) throws SAXException, IOException {
        super.parse(input);
    }

    @Override
    public void parse(String systemId) throws SAXException, IOException {
        super.parse(systemId);
    }

    @Override
    public void startDocument() throws SAXException {
        if (dump) {
            initDump();
        }
        super.startDocument();
        dfHandler.startDocument();
    }

    @Override
    public void endDocument() throws SAXException {
        super.endDocument();
        dfHandler.endDocument();
        if (df != null) {
            try {
                dfOut.close();
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    @Override
    public void startPrefixMapping(String prefix, String uri) throws SAXException {
        super.startPrefixMapping(prefix, uri);
        dfHandler.startPrefixMapping(prefix, uri);
    }

    @Override
    public void endPrefixMapping(String prefix) throws SAXException {
        super.endPrefixMapping(prefix);
        dfHandler.endPrefixMapping(prefix);
    }

    @Override
    public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
        super.startElement(uri, localName, qName, atts);
        dfHandler.startElement(uri, localName, qName, atts);
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        super.endElement(uri, localName, qName);
        dfHandler.endElement(uri, localName, qName);
    }

    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        super.characters(ch, start, length);
        dfHandler.characters(ch, start, length);
    }

    @Override
    public void ignorableWhitespace(char[] ch, int start, int length) throws SAXException {
        super.ignorableWhitespace(ch, start, length);
        dfHandler.ignorableWhitespace(ch, start, length);
    }

    @Override
    public void processingInstruction(String target, String data) throws SAXException {
        super.processingInstruction(target, data);
        dfHandler.processingInstruction(target, data);
    }

    @Override
    public void skippedEntity(String name) throws SAXException {
        super.skippedEntity(name);
        dfHandler.skippedEntity(name);
    }

    @Override
    public void notationDecl(String name, String publicId, String systemId) throws SAXException {
        super.notationDecl(name, publicId, systemId);
        dfHandler.notationDecl(name, publicId, systemId);
    }

    @Override
    public void unparsedEntityDecl(String name, String publicId, String systemId, String notationName) throws SAXException {
        super.unparsedEntityDecl(name, publicId, systemId, notationName);
        dfHandler.unparsedEntityDecl(name, publicId, systemId, notationName);
    }

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
