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

import java.io.BufferedInputStream;
import java.io.PipedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.util.zip.ZipInputStream;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.stream.StreamResult;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import junit.framework.Assert;

/**
 *
 * @author michael
 */
public class JoiningXMLFilterTest {

    private final File testXml = new File("src/test/resources/large.xml.zip");
    
    public JoiningXMLFilterTest() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }

    @Before
    public void setUp() {
    }

    @Test
    public void testParse_InputSource() throws Exception {
        InputStream inRaw = getXmlInputStream(testXml);
        InputStream inTest = getSplitJoinedInputStream(getXmlInputStream(testXml));
        int rawByte;
        int testByte;
        int differingBytes = 0;
        do {
            rawByte = inRaw.read();
            testByte = inTest.read();
            if (rawByte != testByte) {
                differingBytes++;
            }
        } while (rawByte != -1 || testByte != -1);
        Assert.assertEquals(0, differingBytes);
    }

    private InputStream getSplitJoinedInputStream(InputStream in) throws IOException {
        PipedInputStream pis = new PipedInputStream();
        PipedOutputStream pos = new PipedOutputStream(pis);
        TestXMLTransformer testTransformer = new TestXMLTransformer(in, pos);
        Thread t = new Thread(testTransformer);
        t.start();
        return pis;
    }

    private static class TestXMLTransformer implements Runnable {

        private final InputStream in;
        private final PipedOutputStream pos;

        public TestXMLTransformer(InputStream in, PipedOutputStream out) {
            this.in = in;
            pos = out;
        }

        @Override
        public void run() {
            try {
                transform();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        private void transform() throws ParserConfigurationException, SAXException, FileNotFoundException, TransformerConfigurationException, TransformerException, IOException {
            JoiningXMLFilterImpl joiner = new JoiningXMLFilterImpl();
            InputSource inputSource = new InputSource(in);
            Transformer t = TransformerFactory.newInstance().newTransformer();
            t.transform(new SAXSource(joiner, inputSource), new StreamResult(pos));
            pos.close();
        }
    }

    private InputStream getXmlInputStream(File zipFile) throws FileNotFoundException, IOException {
        PipedInputStream pis = new PipedInputStream();
        PipedOutputStream pos = new PipedOutputStream(pis);
        TestXMLExtractor extractor = new TestXMLExtractor(zipFile, pos);
        Thread t = new Thread(extractor);
        t.start();
        return pis;
    }

    private static class TestXMLExtractor implements Runnable {

        private final File f;
        private final PipedOutputStream pos;
        private final byte[] buffer = new byte[2048];

        public TestXMLExtractor(File f, PipedOutputStream pos) {
            this.f = f;
            this.pos = pos;
        }

        @Override
        public void run() {
            ZipInputStream zin = null;
            try {
                zin = new ZipInputStream(new BufferedInputStream(new FileInputStream(f)));
                extract(zin);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            } finally {
                try {
                    if (zin != null) {
                        zin.close();
                    }
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            }
        }

        private void extract(ZipInputStream zin) throws FileNotFoundException, IOException {
            zin.getNextEntry();
            int bytesRead;
            while ((bytesRead = zin.read(buffer)) != -1) {
                pos.write(buffer, 0, bytesRead);
            }
            pos.close();
        }
    }

    @Test
    public void testParse_String() throws Exception {
    }

    private static class JoiningXMLFilterImpl extends JoiningXMLFilter {

        public JoiningXMLFilterImpl() throws ParserConfigurationException, SAXException {
            SplittingXMLFilter splitter = new SplittingXMLFilter();
            setParent(splitter);
            SAXParserFactory spf = SAXParserFactory.newInstance();
            spf.setNamespaceAware(true);
            splitter.setParent(spf.newSAXParser().getXMLReader());
        }

        @Override
        public void parse(InputSource input) throws SAXException, IOException {
            SplittingXMLFilter parent = (SplittingXMLFilter) getParent();
            parent.setContentHandler(this);
            parent.setDTDHandler(this);
            parent.setEntityResolver(this);
            parent.setErrorHandler(this);
            parent.setProperty(LEXICAL_HANDLER_PROPERTY_KEY, this);
            do {
                parent.parse(input);
            } while (parent.hasMoreOutput(input));
            finished();
        }

        @Override
        public void parse(String systemId) throws SAXException, IOException {
            throw new UnsupportedOperationException("Not supported yet.");
        }
    }
}
