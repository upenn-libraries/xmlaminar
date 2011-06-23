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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.ZipInputStream;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 *  Uses a decorator ContentHandler to insert a "tee" into a SAX parse/serialize
 *  stream.
 */
public class SubjectRemediationTableBuilder {

    private SubjectNode table = new SubjectNode();
    private static final Boolean sync = true;

    public static void main(String[] argv)
            throws Exception {
        SubjectRemediationTableBuilder builder = new SubjectRemediationTableBuilder();
        SubjectNode subjectRemediationTable = builder.getSubjectRemediationTable();
        synchronized(sync) {
            System.gc();
            sync.wait(5000);
            subjectRemediationTable = null;
            builder.table = null;
            System.gc();
            sync.wait(5000);
        }
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
                URL rdfUrl = new URL("http://id.loc.gov/static/data/lcsh.rdf.zip");
                InputStream is = rdfUrl.openStream();
                ZipInputStream zis = new ZipInputStream(new BufferedInputStream(is));
                int count;
                byte[] data = new byte[BUFFER];
                while ((count = zis.read(data, 0, BUFFER)) != -1) {
                    buffOut.write(data, 0, count);
                }
                buffOut.close();
            } catch (MalformedURLException ex) {
                Logger.getLogger(SubjectRemediationTableBuilder.class.getName()).log(Level.SEVERE, null, ex);
            } catch (IOException ex) {
                ex.printStackTrace(System.out);
            }
        }

    }

    public SubjectNode getSubjectRemediationTable() throws Exception {
        long start = System.currentTimeMillis();
        BufferedReader bufferedReader = new BufferedReader(new FileReader("/home/michael/Downloads/lcsh-20110104.rdf"));
        //BufferedReader bufferedReader = getURLReader();

        SAXParserFactory spf = SAXParserFactory.newInstance();
        spf.setNamespaceAware(true);
        SAXParser parser = spf.newSAXParser();

        parser.parse(new InputSource(bufferedReader), new MyDefaultHandler());

        bufferedReader.close();
        System.out.println("SAX duration: " + (System.currentTimeMillis() - start));
        table.printBaseSize();
        return table;
    }

    private class MyDefaultHandler extends DefaultHandler {

        private boolean inDescriptionElement = false;
        private boolean inPrefLabelElement = false;
        private boolean inAltLabelElement = false;
        private StringBuilder prefLabel = null;
        private StringBuilder workingAltLabel = null;
        private ArrayList<String> altLabels = null;

        @Override
        public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
            if (!inDescriptionElement) {
                if ("rdf:Description".equals(qName)) {
                    inDescriptionElement = true;
                    prefLabel = new StringBuilder();
                    altLabels = new ArrayList<String>();
                }
            } else {
                if ("skos:prefLabel".equals(qName)) {
                    if (prefLabel.length() > 0) {
                        throw new RuntimeException();
                    }
                    inPrefLabelElement = true;
                } else if ("skos:altLabel".equals(qName)) {
                    workingAltLabel = new StringBuilder();
                    inAltLabelElement = true;
                }
            }
        }

        @Override
        public void endElement(String uri, String localName, String qName) throws SAXException {
            if (inDescriptionElement) {
                if (inPrefLabelElement) {
                    if ("skos:prefLabel".equals(qName)) { // XXX "if" should not be necessary
                        inPrefLabelElement = false;
                    }
                } else if (inAltLabelElement) {
                    if ("skos:altLabel".equals(qName)) { // XXX "if" should not be necessary
                        inAltLabelElement = false;
                        commitSingleAltLabel();
                    }
                } else {
                    if ("rdf:Description".equals(qName)) {
                        inDescriptionElement = false;
                        commit();
                    }
                }
            }
        }

        @Override
        public void characters(char[] ch, int start, int length) throws SAXException {
            if (inDescriptionElement) {
                if (inPrefLabelElement) {
                    prefLabel.append(java.nio.CharBuffer.wrap(ch, start, length));
                } else if (inAltLabelElement) {
                    workingAltLabel.append(java.nio.CharBuffer.wrap(ch, start, length));
                }
            }
        }

        private void commitSingleAltLabel() {
            altLabels.add(workingAltLabel.toString());
        }

        private void commit() {
            if (!altLabels.isEmpty()) {
//                System.out.println();
//                System.out.println(prefLabel);
                boolean subDivision = false;
                boolean absoluteHeading = false;
                String prefLabelString = prefLabel.toString();
                for (String altLabel : altLabels) {
                    if (altLabel.startsWith("--")) {
                        subDivision = true;
                    } else {
                        absoluteHeading = true;
                    }
//                    System.out.println("\t" + altLabel);
                    table.put(altLabel, prefLabelString);
                }

                if (subDivision && absoluteHeading) {
                    throw new RuntimeException("unexpected subdivision/main heading combination");
                }
            }
        }
    }
}
