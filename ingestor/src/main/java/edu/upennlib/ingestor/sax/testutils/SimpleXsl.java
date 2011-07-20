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

import edu.upennlib.ingestor.sax.utils.NoopXMLFilter;
import edu.upennlib.ingestor.sax.xsl.BufferingXMLFilter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.Templates;
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
public class SimpleXsl {

    public static final String TRANSFORMER_FACTORY_CLASS_NAME = "net.sf.saxon.TransformerFactoryImpl";
    public static void main(String[] args) throws TransformerConfigurationException, TransformerException, SAXException, ParserConfigurationException {
        TransformerFactory tf = TransformerFactory.newInstance(TRANSFORMER_FACTORY_CLASS_NAME, null);
        Templates templates = tf.newTemplates(new StreamSource(new File("inputFiles/fullTest.xsl")));
        Transformer t = templates.newTransformer();
        Transformer tNoop = tf.newTransformer();
        BufferingXMLFilter buff = new BufferingXMLFilter();
        SAXParserFactory spf = SAXParserFactory.newInstance();
        spf.setNamespaceAware(true);
        NoopXMLFilter nxf = new NoopXMLFilter();
        nxf.setParent(spf.newSAXParser().getXMLReader());
        buff.setParent(nxf);
        tNoop.transform(new StreamSource("inputFiles/test.xml"), new SAXResult(buff));
        t.transform(new SAXSource(buff, new InputSource()), new StreamResult(System.out));
    }
}
