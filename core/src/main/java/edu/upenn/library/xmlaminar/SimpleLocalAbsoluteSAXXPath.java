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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.StringWriter;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

/**
 *
 * @author michael
 */
public class SimpleLocalAbsoluteSAXXPath {

    public static final String MARCXML_RECORD_XPATH = "/collection/record/controlfield[@tag=001]";
    public static final String INTEGRATOR_RECORD_XPATH = "/root/record[@id]";

    private final RecordXPathContentHandler rxpch;

    public SimpleLocalAbsoluteSAXXPath(String xpath) {
        rxpch = new RecordXPathContentHandler(xpath);
    }

    public String evaluate(XMLReader r, InputSource input) throws RecordIdNotAvailableException, IOException, SAXException {
        rxpch.clear();
        r.setContentHandler(rxpch);
        try {
            r.parse(input);
        } catch (FoundXPathException ex) {
        }
        return rxpch.getRecordId();
    }

    private static final File INTEGRATOR_FILE = new File("/home/michael/NetBeansProjects/synch-branch/inputFiles/test.xml");
    private static final File MARCXML_FILE = new File("/home/michael/NetBeansProjects/synch-branch/inputFiles/large_bad.xml");

    public static void main(String[] args) throws ParserConfigurationException, SAXException, IOException {
        SimpleLocalAbsoluteSAXXPath instance = new SimpleLocalAbsoluteSAXXPath(INTEGRATOR_RECORD_XPATH);
        SAXParserFactory spf = SAXParserFactory.newInstance();
        spf.setNamespaceAware(true);
        String id = instance.evaluate(spf.newSAXParser().getXMLReader(), new InputSource(new FileInputStream(INTEGRATOR_FILE)));
        System.out.println(id);
    }

    private static class RecordXPathContentHandler implements ContentHandler {

        private final boolean[] attribute;
        private final String[] localNames;
        private final String[] attValues;

        private int level = -1;
        private int trueLevel = -1;
        private int testIndex = 0;
        private boolean found = false;
        private StringWriter returnValue;
        private final FoundXPathException recordIdFoundException = new FoundXPathException();
        private final RecordIdNotAvailableException recordIdNotAvailableException = new RecordIdNotAvailableException();

        public RecordXPathContentHandler(String xpath) {
            if (!xpath.startsWith("/")) {
                throw new IllegalArgumentException("simple xpath must be absolute");
            }
            if (xpath.contains("\"") || xpath.contains("'")) {
                throw new IllegalArgumentException("simple xpath does not use quotes");
            }
            if (xpath.matches("][^/]")) {
                throw new IllegalArgumentException("simple xpath does not support multiple attribute conditions");
            }
            xpath = xpath.substring(1);
            String[] args = xpath.split("(]$)|(]?/)|\\[");
            attribute = new boolean[args.length];
            localNames = new String[args.length];
            attValues = new String[args.length];
            for (int i = 0; i < args.length; i++) {
                String s = args[i];
                if (s.startsWith("@")) {
                    attribute[i] = true;
                    s = s.substring(1);
                    String[] nameValue = s.split("=");
                    localNames[i] = nameValue[0];
                    if (nameValue.length > 1) {
                        attValues[i] = nameValue[1];
                    }
                } else {
                    attribute[i] = false;
                    localNames[i] = s;
                }
            }
        }

        public void clear() {
            level = -1;
            trueLevel = -1;
            testIndex = 0;
            returnValue = new StringWriter();
            found = false;
        }

        public String getRecordId() throws RecordIdNotAvailableException {
            if (found) {
                return returnValue.toString().trim();
            } else {
                throw recordIdNotAvailableException;
            }
        }

        @Override
        public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
            if (trueLevel == level) {
                boolean match = false;
                if (localName.equals(localNames[testIndex])) {
                    testIndex++;
                    if (attribute[testIndex]) {
                        boolean attMatch = false;
                        for (int i = 0; i < atts.getLength(); i++) {
                            if (atts.getLocalName(i).equals(localNames[testIndex])) {
                                if (attValues[testIndex] != null) {
                                    if (atts.getValue(i).equals(attValues[testIndex])) {
                                        attMatch = true;
                                        match = true;
                                    }
                                } else {
                                    found = true;
                                    returnValue.write(atts.getValue(i));
                                    throw recordIdFoundException;
                                }
                            }
                        }
                        if (attMatch) {
                            testIndex++;
                        } else {
                            testIndex--;
                        }
                    } else {
                        match = true;
                    }
                }
                if (match) {
                    if (testIndex >= localNames.length) {
                        found = true;
                    }
                    trueLevel++;
                }
            }
            level++;
        }

        @Override
        public void endElement(String uri, String localName, String qName) throws SAXException {
            if (found) {
                throw recordIdFoundException;
            } else if (trueLevel == level) {
                trueLevel--;
                do {
                    testIndex--;
                } while (attribute[testIndex]);
            }
            level--;
        }

        @Override
        public void characters(char[] ch, int start, int length) throws SAXException {
            if (found) {
                returnValue.write(ch, start, length);
            }
        }

        @Override
        public void setDocumentLocator(Locator locator) {
        }

        @Override
        public void startDocument() throws SAXException {
        }

        @Override
        public void endDocument() throws SAXException {
        }

        @Override
        public void startPrefixMapping(String prefix, String uri) throws SAXException {
        }

        @Override
        public void endPrefixMapping(String prefix) throws SAXException {
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
    }

    private static class FoundXPathException extends SAXException {

        @Override
        public synchronized Throwable fillInStackTrace() {
            return null;
        }

    }

    public static class RecordIdNotAvailableException extends SAXException {

        @Override
        public synchronized Throwable fillInStackTrace() {
            return null;
        }

    }

}
