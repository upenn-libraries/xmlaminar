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

package edu.upennlib.paralleltransformer;

import edu.upennlib.xmlutils.DevNullContentHandler;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.concurrent.Executors;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.stream.StreamResult;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLFilterImpl;

/**
 *
 * @author Michael Gibney
 */
public class NewClass {

    public static void main(String[] args) throws InterruptedException, ParserConfigurationException, SAXException, FileNotFoundException, IOException, TransformerConfigurationException, TransformerException {
        TransformerFactory tf = TransformerFactory.newInstance("net.sf.saxon.TransformerFactoryImpl", null);
        Transformer t = tf.newTransformer();
        final JoiningXMLFilter joiner = new JoiningXMLFilter();
        joiner.setInputType(QueueSourceXMLFilter.InputType.queue);
        LevelSplittingXMLFilter sxf = new LevelSplittingXMLFilter();
        sxf.setInputType(QueueSourceXMLFilter.InputType.indirect);
        sxf.setChunkSize(1);
        SAXParserFactory spf = SAXParserFactory.newInstance();
        spf.setNamespaceAware(true);
        SAXParser sp = spf.newSAXParser();
        XMLReader xmlReader = sp.getXMLReader();
        String testXML = "<a:a xmlns:a=\"http://whatever\"/>";
        ContentHandler blah = new DevNullContentHandler() {

            @Override
            public void endElement(String uri, String localName, String qName) throws SAXException {
                System.out.println("endElement: "+localName);
                super.endElement(uri, localName, qName); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
                System.out.println("startElement: "+localName);
                super.startElement(uri, localName, qName, atts); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public void endPrefixMapping(String prefix) throws SAXException {
                System.out.println("endPrefix: "+prefix);
                super.endPrefixMapping(prefix); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public void startPrefixMapping(String prefix, String uri) throws SAXException {
                System.out.println("startPrefix: "+uri);
                super.startPrefixMapping(prefix, uri); //To change body of generated methods, choose Tools | Templates.
            }
            
        };
        xmlReader.setContentHandler(blah);
        xmlReader.parse(new InputSource(new StringReader(testXML)));
        System.exit(0);
        joiner.setParent(sxf);
        sxf.setParent(xmlReader);
        sxf.setXMLReaderCallback(new SplittingXMLFilter.XMLReaderCallback() {
            int i = 0;
            @Override
            public void callback(XMLReader reader, InputSource input) throws SAXException, IOException {
                try {
                    joiner.getParseQueue().put(new SAXSource(reader, input));
                    System.out.println("what "+input.getSystemId()+", "+i++);
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
            }

            @Override
            public void callback(XMLReader reader, String systemId) throws SAXException, IOException {
                throw new UnsupportedOperationException("Not supported yet.");
            }

            @Override
            public void finished() {
                try {
                    joiner.getParseQueue().put(QueueSourceXMLFilter.FINISHED);
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
            }
        });
        File in = new File("blah.txt");
        InputSource inSource = new InputSource(new FileReader(in));
        inSource.setSystemId(in.getPath());
        sxf.setExecutor(Executors.newCachedThreadPool());
        try {
            t.transform(new SAXSource(joiner, inSource), new StreamResult(System.out));
        } finally {
            sxf.shutdown();
            joiner.shutdown();
        }
    }

}
