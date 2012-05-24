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

package edu.upennlib.ingestor.sax.integrator;

import edu.upennlib.xmlutils.UnboundedContentHandlerBuffer;
import javax.xml.transform.sax.SAXResult;
import java.nio.channels.Channels;
import org.w3c.dom.Document;
import javax.xml.parsers.DocumentBuilder;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.transform.TransformerException;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.sax.SAXTransformerFactory;
import javax.xml.transform.TransformerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.sax.SAXSource;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.XMLFilterImpl;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author michael
 */
public class IntegratorOutputNodeTest {

    private static final ClassLoader cl = IntegratorOutputNodeTest.class.getClassLoader();

    public IntegratorOutputNodeTest() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
        BasicConfigurator.configure();
        Logger mainLogger = Logger.getLogger(IntegratorOutputNode.class);
        mainLogger.setLevel(Level.DEBUG);
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }

    @Before
    public void setUp() {
    }

    /*
     * Simple interleaving of even and odd record ids, no required nodes.
     */
    @Test
    public void testIntegrate1() throws TransformerConfigurationException, TransformerException, ParserConfigurationException, SAXException, IOException {
        String testId = "testIntegrate1";
        System.out.println("running test: "+testId);
        IntegratorOutputNode root = new IntegratorOutputNode();
        root.addDescendent("/record/marc", new PreConfiguredXMLReader(new InputSource(cl.getResourceAsStream("input/marc.xml"))), false);
        root.addDescendent("/record/marcOdd", new PreConfiguredXMLReader(new InputSource(cl.getResourceAsStream("input/marcOdd.xml"))), false);
        verify(root, testId.concat(".xml"), true);
    }

    /*
     * Interleaving with some overlap, some no-overlap, aggregating hldgs.
     */
    @Test
    public void testIntegrate2() throws TransformerConfigurationException, TransformerException, ParserConfigurationException, SAXException, IOException {
        String testId = "testIntegrate2";
        System.out.println("running test: "+testId);
        IntegratorOutputNode root = new IntegratorOutputNode();
        root.addDescendent("/record/marc", new PreConfiguredXMLReader(new InputSource(cl.getResourceAsStream("input/marc.xml"))), false);
        root.addDescendent("/record/hldgs/hldg", new PreConfiguredXMLReader(new InputSource(cl.getResourceAsStream("input/hldg.xml"))), false);
        verify(root, testId.concat(".xml"), true);
    }

    /*
     * Interleaving with some overlap, some no-overlap, no required nodes
     */
    @Test
    public void testIntegrate3() throws TransformerConfigurationException, TransformerException, ParserConfigurationException, SAXException, IOException {
        String testId = "testIntegrate3";
        System.out.println("running test: "+testId);
        IntegratorOutputNode root = new IntegratorOutputNode();
        root.addDescendent("/record/marc", new PreConfiguredXMLReader(new InputSource(cl.getResourceAsStream("input/marc.xml"))), false);
        root.addDescendent("/record/marcSpecial", new PreConfiguredXMLReader(new InputSource(cl.getResourceAsStream("input/marcSpecial.xml"))), false);
        verify(root, testId.concat(".xml"), true);
    }

    /*
     * Interleaving with empty, no-overlap, no required nodes
     */
    @Test
    public void testIntegrate4() throws TransformerConfigurationException, TransformerException, ParserConfigurationException, SAXException, IOException {
        String testId = "testIntegrate4";
        System.out.println("running test: "+testId);
        IntegratorOutputNode root = new IntegratorOutputNode();
        root.addDescendent("/record/marc", new PreConfiguredXMLReader(new InputSource(cl.getResourceAsStream("input/marc.xml"))), false);
        root.addDescendent("/record/marcEmpty", new PreConfiguredXMLReader(new InputSource(cl.getResourceAsStream("input/marcEmpty.xml"))), false);
        verify(root, testId.concat(".xml"), true);
    }

    /*
     * Interleaving with empty, no-overlap, empty stream required
     */
    @Test
    public void testIntegrate5() throws TransformerConfigurationException, TransformerException, ParserConfigurationException, SAXException, IOException {
        String testId = "testIntegrate5";
        System.out.println("running test: "+testId);
        IntegratorOutputNode root = new IntegratorOutputNode();
        root.addDescendent("/record/marc", new PreConfiguredXMLReader(new InputSource(cl.getResourceAsStream("input/marc.xml"))), false);
        root.addDescendent("/record/marcEmpty", new PreConfiguredXMLReader(new InputSource(cl.getResourceAsStream("input/marcEmpty.xml"))), true);
        verify(root, testId.concat(".xml"), true);
    }

    /*
     * Interleaving with empty, no-overlap, empty stream required
     */
    @Test
    public void testIntegrate6() throws TransformerConfigurationException, TransformerException, ParserConfigurationException, SAXException, IOException {
        String testId = "testIntegrate6";
        System.out.println("running test: "+testId);
        IntegratorOutputNode root = new IntegratorOutputNode();
        root.addDescendent("/record/marcEmpty1", new PreConfiguredXMLReader(new InputSource(cl.getResourceAsStream("input/marcEmpty.xml"))), false);
        root.addDescendent("/record/marcEmpty2", new PreConfiguredXMLReader(new InputSource(cl.getResourceAsStream("input/marcEmpty.xml"))), false);
        verify(root, testId.concat(".xml"), true);
    }

    /*
     * Interleaving with empty, no-overlap, empty stream required
     */
    @Test
    public void testIntegrate7() throws TransformerConfigurationException, TransformerException, ParserConfigurationException, SAXException, IOException {
        String testId = "testIntegrate7";
        System.out.println("running test: "+testId);
        IntegratorOutputNode root = new IntegratorOutputNode();
        root.addDescendent("/record/marc", new PreConfiguredXMLReader(new InputSource(cl.getResourceAsStream("input/marc.xml"))), true);
        root.addDescendent("/record/marcOdd", new PreConfiguredXMLReader(new InputSource(cl.getResourceAsStream("input/marcOdd.xml"))), true);
        verify(root, testId.concat(".xml"), true);
    }

    /*
     * Interleaving with empty, no-overlap, empty stream required
     */
    @Test
    public void testIntegrate8() throws TransformerConfigurationException, TransformerException, ParserConfigurationException, SAXException, IOException {
        String testId = "testIntegrate8";
        System.out.println("running test: "+testId);
        IntegratorOutputNode root = new IntegratorOutputNode();
        root.addDescendent("/record/marc", new PreConfiguredXMLReader(new InputSource(cl.getResourceAsStream("input/marc.xml"))), false);
        root.addDescendent("/record/hldgs/hldg", new PreConfiguredXMLReader(new InputSource(cl.getResourceAsStream("input/hldg.xml"))), false);
        root.addDescendent("/record/hldgs/hldg/items/itemEven", new PreConfiguredXMLReader(new InputSource(cl.getResourceAsStream("input/itemEven.xml"))), false);
        verify(root, testId.concat(".xml"), true);
    }

    @Test
    public void testIntegrate9() throws TransformerConfigurationException, TransformerException, ParserConfigurationException, SAXException, IOException {
        String testId = "testIntegrate9";
        System.out.println("running test: "+testId);
        IntegratorOutputNode root = new IntegratorOutputNode();
        root.addDescendent("/record/marc", new PreConfiguredXMLReader(new InputSource(cl.getResourceAsStream("input/real/marc.xml"))), false);
        root.addDescendent("/record/holdings/holding", new PreConfiguredXMLReader(new InputSource(cl.getResourceAsStream("input/real/hldg.xml"))), false);
        root.addDescendent("/record/holdings/holding/items/item", new PreConfiguredXMLReader(new InputSource(cl.getResourceAsStream("input/real/item.xml"))), false);
        root.addDescendent("/record/holdings/holding/items/item/itemStatuses/itemStatus", new PreConfiguredXMLReader(new InputSource(cl.getResourceAsStream("input/real/itemStatus.xml"))), false);
        verify(root, testId.concat(".xml"), true);
    }

    /*
     * Interleaving with empty, no-overlap, empty stream required
     */
    @Test
    public void testIntegrate10() throws TransformerConfigurationException, TransformerException, ParserConfigurationException, SAXException, IOException {
        String testId = "testIntegrate10";
        System.out.println("running test: "+testId);
        IntegratorOutputNode root = new IntegratorOutputNode();
        root.addDescendent("/itemEven", new PreConfiguredXMLReader(new InputSource(cl.getResourceAsStream("input/simpleItemEven.xml"))), false);
        root.addDescendent("/itemOdd", new PreConfiguredXMLReader(new InputSource(cl.getResourceAsStream("input/simpleItemOdd.xml"))), false);
        verify(root, testId.concat(".xml"), true);
    }

    private static void verify(IntegratorOutputNode root, String fileName, boolean verify) throws TransformerConfigurationException, TransformerException, IOException, ParserConfigurationException, SAXException {
        SAXTransformerFactory tf = (SAXTransformerFactory) TransformerFactory.newInstance("net.sf.saxon.TransformerFactoryImpl", null);
        Transformer t = tf.newTransformer();
        t.setOutputProperty(OutputKeys.INDENT, "yes");
        ByteArrayOutputStream actual = null;
        File outputFile = null;
        OutputStream out;
        if (verify) {
            actual = new ByteArrayOutputStream();
            out = actual;
        } else {
            outputFile = new File("./src/test/resources/output/".concat(fileName));
            out = new FileOutputStream(outputFile);
        }
        t.transform(new SAXSource(root, new InputSource()), new StreamResult(out));
        out.close();
        if (verify) {
            assertTrue(xmlEquals(new ByteArrayInputStream(actual.toByteArray()), cl.getResourceAsStream("output/".concat(fileName))));
        } else {
            new FileInputStream(outputFile).getChannel().transferTo(0, outputFile.length(), Channels.newChannel(System.out));
        }
    }

    private static void verifyTest(IntegratorOutputNode root, String fileName, boolean verify) throws TransformerConfigurationException, TransformerException, IOException, ParserConfigurationException, SAXException {
        SAXTransformerFactory tf = (SAXTransformerFactory) TransformerFactory.newInstance("net.sf.saxon.TransformerFactoryImpl", null);
        Transformer t = tf.newTransformer();
        t.setOutputProperty(OutputKeys.INDENT, "yes");
        UnboundedContentHandlerBuffer out = new UnboundedContentHandlerBuffer();
        try {
            t.transform(new SAXSource(root, new InputSource()), new SAXResult(out));
        } catch (Exception ex) {
            ex.printStackTrace(System.err);
        }
        out.dump(System.out, false);
    }

    private static boolean xmlEquals(InputStream actual, InputStream expected) throws ParserConfigurationException, SAXException, IOException {
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        dbf.setNamespaceAware(true);
        dbf.setCoalescing(true);
        dbf.setIgnoringElementContentWhitespace(true);
        dbf.setIgnoringComments(true);
        DocumentBuilder db = dbf.newDocumentBuilder();
        Document docActual = db.parse(actual);
        Document docExpected = db.parse(expected);
        docActual.normalizeDocument();
        docExpected.normalizeDocument();
        return docActual.isEqualNode(docExpected);
    }

    private static class PreConfiguredXMLReader extends XMLFilterImpl {

        private final InputSource in;

        private PreConfiguredXMLReader(InputSource in) throws ParserConfigurationException, SAXException {
            this.in = in;
            SAXParserFactory spf = SAXParserFactory.newInstance();
            spf.setNamespaceAware(true);
            SAXParser sp = spf.newSAXParser();
            setParent(sp.getXMLReader());
        }

        @Override
        public void parse(InputSource input) throws SAXException, IOException {
            super.parse(in);
        }

        @Override
        public void parse(String systemId) throws SAXException, IOException {
            super.parse(in);
        }

    }

}