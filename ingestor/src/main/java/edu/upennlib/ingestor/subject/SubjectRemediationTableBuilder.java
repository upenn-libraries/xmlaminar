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

package edu.upennlib.ingestor.subject;

import edu.upennlib.xmlutils.SAXFeatures;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.CharArrayWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.zip.ZipInputStream;

import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

/**
 *  Uses a decorator ContentHandler to insert a "tee" into a SAX parse/serialize
 *  stream.
 */
public class SubjectRemediationTableBuilder {

    public static void main(String[] argv) throws Exception {
        SubjectRemediationTableBuilder builder = new SubjectRemediationTableBuilder();
        long start = System.currentTimeMillis();
        SubjectNode table = builder.getSubjectRemediationTable();
        System.out.println("SAX duration: " + (System.currentTimeMillis() - start));
        table.printBaseSize();
        Runtime runtime = Runtime.getRuntime();
        runtime.gc();
        System.out.println("memory used: "+((runtime.totalMemory() - runtime.freeMemory()) / 1048576)+" MB");
        ObjectOutputStream oos = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream("/tmp/serialized.obj")));
        oos.writeObject(table);
        oos.close();
        oos = null;
        table = null;
        runtime.gc();
        System.out.println("memory used: "+((runtime.totalMemory() - runtime.freeMemory()) / 1048576)+" MB");
        ObjectInputStream ois = new ObjectInputStream(new BufferedInputStream(new FileInputStream("/tmp/serialized.obj")));
        start = System.currentTimeMillis();
        table = (SubjectNode) ois.readObject();
        ois.close();
        System.out.println("fromDisk duration: " + (System.currentTimeMillis() - start));
        table.printBaseSize();
        ois = null;
        runtime.gc();
        System.out.println("memory used: "+((runtime.totalMemory() - runtime.freeMemory()) / 1048576)+" MB");
    }

    private BufferedReader getURLReader() throws MalformedURLException, IOException {
        PipedInputStream pis = new PipedInputStream();
        FileDownloader fd = new FileDownloader(new PipedOutputStream(pis));
        Thread t = new Thread(fd);
        t.start();
        return new BufferedReader(new InputStreamReader(pis));
    }

    private class FileDownloader implements Runnable {

        PipedOutputStream pos = null;
        BufferedOutputStream buffOut = null;
        int BUFFER = 2048;

        public FileDownloader(PipedOutputStream pos) {
            this.pos = pos;
            buffOut = new BufferedOutputStream(pos);
        }

        @Override
        public void run() {
            try {
                URL rdfUrl = new URL("http://id.loc.gov/static/data/authoritiessubjects.rdfxml.skos.zip");
                InputStream is = rdfUrl.openStream();
                ZipInputStream zis = new ZipInputStream(new BufferedInputStream(is));
                int count;
                byte[] data = new byte[BUFFER];
                zis.getNextEntry();
                while ((count = zis.read(data, 0, BUFFER)) != -1) {
                    buffOut.write(data, 0, count);
                }
                buffOut.close();
            } catch (MalformedURLException ex) {
                throw new RuntimeException(ex);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }

    }

    public SubjectNode getSubjectRemediationTable() throws Exception {
        SAXParserFactory spf = SAXParserFactory.newInstance();
        spf.setNamespaceAware(true);
        XMLReader parser = spf.newSAXParser().getXMLReader();
        SubjectTableBuildingHandler stbh = new SubjectTableBuildingHandler(parser.getFeature(SAXFeatures.STRING_INTERNING));
        parser.setContentHandler(stbh);

        BufferedReader bufferedReader = new BufferedReader(new FileReader("/home/michael/Downloads/lcsh-20110104.rdf"));
        //BufferedReader bufferedReader = getURLReader();
        stbh.setTable(new SubjectNode());
        parser.parse(new InputSource(bufferedReader));
        SubjectNode table = stbh.retrieveTable();

        bufferedReader.close();
        return table;
    }

    private static class SubjectTableBuildingHandler implements ContentHandler {

        private SubjectNode table;
        private boolean inDescriptionElement = false;
        private boolean inPrefLabelElement = false;
        private boolean inAltLabelElement = false;
        private final CharArrayWriter prefLabel = new CharArrayWriter();
        private final CharArrayWriter workingAltLabel = new CharArrayWriter();
        private final ArrayList<String> altLabels = new ArrayList<String>();
        private static final boolean STRICT = false;

        public SubjectTableBuildingHandler(boolean interning) {
            if (!interning) {
                throw new IllegalArgumentException();
            }
        }

        private static final String RDF_DESCRIPTION_QNAME = "rdf:Description";
        private static final String SKOS_PREFLABEL_QNAME = "skos:prefLabel";
        private static final String SKOS_ALTLABEL_QNAME = "skos:altLabel";

        @Override
        public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
            if (!inDescriptionElement) {
                if (RDF_DESCRIPTION_QNAME == qName) {
                    inDescriptionElement = true;
                    prefLabel.reset();
                    altLabels.clear();
                } else if (STRICT && RDF_DESCRIPTION_QNAME.equals(qName)) {
                    throw new RuntimeException();
                }
            } else {
                if (SKOS_PREFLABEL_QNAME == qName) {
                    if (STRICT && prefLabel.size() > 0) {
                        throw new RuntimeException();
                    }
                    inPrefLabelElement = true;
                } else if (SKOS_ALTLABEL_QNAME == qName) {
                    workingAltLabel.reset();
                    inAltLabelElement = true;
                } else if (STRICT && (SKOS_PREFLABEL_QNAME.equals(qName) || SKOS_ALTLABEL_QNAME.equals(qName))) {
                    throw new RuntimeException();
                }
            }
        }

        @Override
        public void endElement(String uri, String localName, String qName) throws SAXException {
            if (inDescriptionElement) {
                if (inPrefLabelElement) {
                    if (!STRICT || SKOS_PREFLABEL_QNAME == qName) {
                        inPrefLabelElement = false;
                    } else {
                        throw new IllegalStateException(qName);
                    }
                } else if (inAltLabelElement) {
                    if (!STRICT || SKOS_ALTLABEL_QNAME == qName) {
                        inAltLabelElement = false;
                        commitSingleAltLabel();
                    } else {
                        throw new IllegalStateException(qName);
                    }
                } else {
                    if (RDF_DESCRIPTION_QNAME == qName) {
                        inDescriptionElement = false;
                        commit();
                    } else if (STRICT && RDF_DESCRIPTION_QNAME.equals(qName)) {
                        throw new IllegalStateException(qName);
                    }
                }
            }
        }

        @Override
        public void characters(char[] ch, int start, int length) throws SAXException {
            if (inDescriptionElement) {
                if (inPrefLabelElement) {
                    prefLabel.write(ch, start, length);
                } else if (inAltLabelElement) {
                    workingAltLabel.write(ch, start, length);
                }
            }
        }

        private void commitSingleAltLabel() {
            altLabels.add(workingAltLabel.toString().intern());
        }

        private void commit() {
            if (!altLabels.isEmpty()) {
                boolean subDivision = false;
                boolean absoluteHeading = false;
                String prefLabelString = prefLabel.toString().intern();
                for (String altLabel : altLabels) {
                    if (altLabel.startsWith("--")) {
                        subDivision = true;
                    } else {
                        absoluteHeading = true;
                    }
                    table.put(altLabel, prefLabelString);
                }

                if (subDivision && absoluteHeading) {
                    throw new RuntimeException("unexpected subdivision/main heading combination");
                }
            }
        }

        private void setTable(SubjectNode table) {
            this.table = table;
        }
        
        private SubjectNode retrieveTable() {
            SubjectNode toReturn = table;
            table = null;
            return toReturn;
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
}
