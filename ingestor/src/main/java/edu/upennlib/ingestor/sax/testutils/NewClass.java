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

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package edu.upennlib.ingestor.sax.testutils;

import edu.upennlib.ingestor.sax.xsl.BoundedXMLFilterBuffer;
import edu.upennlib.ingestor.sax.xsl.BufferingXMLFilter;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.sax.SAXResult;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 *
 * @author michael
 */
public class NewClass {

    public static void main(String[] args) throws ParserConfigurationException, SAXException, TransformerConfigurationException, TransformerException, FileNotFoundException, IOException {
        SAXParserFactory spf = SAXParserFactory.newInstance();
        spf.setNamespaceAware(true);
        BoundedXMLFilterBuffer buffer = new BoundedXMLFilterBuffer();
        BufferingXMLFilter oldBuffer = new BufferingXMLFilter();
        FileInputStream fis = new FileInputStream("inputFiles/large.xml");
        BufferedInputStream bis = new BufferedInputStream(fis);
        FileOutputStream fos = new FileOutputStream("/tmp/testing.xml");
        BufferedOutputStream bos = new BufferedOutputStream(fos);

        oldBuffer.setParent(spf.newSAXParser().getXMLReader());
        buffer.setParent(spf.newSAXParser().getXMLReader());

        TransformerFactory tf = TransformerFactory.newInstance();
        Transformer t = tf.newTransformer();
        long start = System.currentTimeMillis();
        boolean buffering = true;
        if (buffering) {
            t.transform(new SAXSource(buffer, new InputSource(bis)), new StreamResult(bos));
        } else {
            t.transform(new StreamSource(bis), new StreamResult(bos));
        }
        bos.close();
        System.out.println("duration: "+(System.currentTimeMillis() - start));
    }
}
