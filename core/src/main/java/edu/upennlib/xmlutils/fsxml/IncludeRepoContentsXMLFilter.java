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

package edu.upennlib.xmlutils.fsxml;

import edu.upennlib.xmlutils.DocEventIgnorer;
import edu.upennlib.xmlutils.XMLInputValidator;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLFilter;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLFilterImpl;

/**
 *
 * @author Michael Gibney
 */
public class IncludeRepoContentsXMLFilter extends XMLFilterImpl {

    private final SAXParserFactory spf = SAXParserFactory.newInstance();
    private SAXParser parser;
    private final XMLFilter subReader = new DocEventIgnorer();

    private void initSubReader() throws SAXException {
        XMLReader prototype = getParent();
        spf.setNamespaceAware(prototype.getFeature("http://xml.org/sax/features/namespaces"));
        spf.setValidating(prototype.getFeature("http://xml.org/sax/features/validation"));
        try {
            spf.setFeature("http://xml.org/sax/features/namespace-prefixes", prototype.getFeature("http://xml.org/sax/features/namespace-prefixes"));
        } catch (ParserConfigurationException ex) {
            throw new SAXException(ex);
        }
        try {
            parser = spf.newSAXParser();
        } catch (ParserConfigurationException ex) {
            throw new SAXException(ex);
        }
        subReader.setParent(parser.getXMLReader());
    }

    @Override
    public void parse(InputSource input) throws SAXException, IOException {
        initSubReader();
        super.parse(input);
    }

    @Override
    public void parse(String systemId) throws SAXException, IOException {
        initSubReader();
        super.parse(systemId);
    }



    @Override
    public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
        super.startElement(uri, localName, qName, atts);
        FilesystemXMLReader.FsxmlElement fileElement = FilesystemXMLReader.FsxmlElement.file;
        if (fileElement.uri.equals(uri) && fileElement.localName.equals(localName)) {
            FilesystemXMLReader.FsxmlAttribute absolutePathAttribute = FilesystemXMLReader.FsxmlAttribute.absolutePath;
            String absolutePath = atts.getValue(absolutePathAttribute.uri, absolutePathAttribute.localName);
            try {
                if (absolutePath.endsWith(".xml")) {
                    incorporateXMLContent(absolutePath);
                } else if (absolutePath.endsWith(".txt") || absolutePath.endsWith(".log")) {
                    incorporateTextContent(absolutePath);
                }
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    private void incorporateXMLContent(String absolutePath) throws SAXException, IOException {
        parser.reset();
        subReader.setContentHandler(this);
        Reader r = new FileReader(absolutePath);
        try {
            InputSource subIn = new InputSource(new XMLInputValidator(r));
            subIn.setSystemId(absolutePath);
            subReader.parse(subIn);
        } finally {
            r.close();
        }
    }

    private final char[] BUFF = new char[1024];

    private void incorporateTextContent(String absolutePath) throws SAXException, IOException {
        Reader r = new FileReader(absolutePath);
        try {
            int read;
            while ((read = r.read(BUFF)) != -1) {
                characters(BUFF, 0, read);
            }
        } finally {
            r.close();
        }
    }

}
