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

package edu.upennlib.ingestor.sax.integrator.complex;

import edu.upennlib.ingestor.sax.integrator.BinaryMARCXMLReader;
import edu.upennlib.ingestor.sax.xsl.BufferingXMLFilter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.stream.StreamResult;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 *
 * @author michael
 */
public class IntegratorSAX {
    private final IntegratorOutputNode rootOutputNode = new IntegratorOutputNode(null);

    public static void main(String[] args) throws ParserConfigurationException, SAXException, FileNotFoundException, IOException, TransformerConfigurationException, TransformerException {
        IntegratorSAX instance = new IntegratorSAX();
        instance.setupAndRun();
    }

    private void setupAndRun() throws ParserConfigurationException, SAXException, FileNotFoundException, TransformerConfigurationException, TransformerException {
        File marcFile = new File("inputFiles/integrator_xml/marc.xml");
        File hldgFile = new File("inputFiles/integrator_xml/mfhd.xml");
        File itemFile = new File("inputFiles/integrator_xml/item.xml");
        File itemStatusFile = new File("inputFiles/integrator_xml/item_status.xml");
        StatefulXMLFilter marcSxf = new StatefulXMLFilter();
        StatefulXMLFilter hldgSxf = new StatefulXMLFilter();
        StatefulXMLFilter itemSxf = new StatefulXMLFilter();
        StatefulXMLFilter itemStatusSxf = new StatefulXMLFilter();
        boolean fromDatabase = false;
        if (fromDatabase) {
            BinaryMARCXMLReader bmxr = new BinaryMARCXMLReader();
            // set properties...
            marcSxf.setParent(bmxr);
        } else {
            // From file
            SAXParserFactory spf = SAXParserFactory.newInstance();
            spf.setNamespaceAware(true);
            marcSxf.setParent(spf.newSAXParser().getXMLReader());
            marcSxf.setInputSource(new InputSource(new FileInputStream(marcFile)));
            hldgSxf.setParent(spf.newSAXParser().getXMLReader());
            hldgSxf.setInputSource(new InputSource(new FileInputStream(hldgFile)));
            itemSxf.setParent(spf.newSAXParser().getXMLReader());
            itemSxf.setInputSource(new InputSource(new FileInputStream(itemFile)));
            itemStatusSxf.setParent(spf.newSAXParser().getXMLReader());
            itemStatusSxf.setInputSource(new InputSource(new FileInputStream(itemStatusFile)));
        }
        ArrayList<StatefulXMLFilter> sxfs = new ArrayList<StatefulXMLFilter>();
        sxfs.add(marcSxf);
        TransformerFactory tf = TransformerFactory.newInstance(TRANSFORMER_FACTORY_CLASS_NAME, null);
        Transformer t = tf.newTransformer();
        t.setOutputProperty(OutputKeys.INDENT, "yes");

        IntegratorOutputNode recordNode = new IntegratorOutputNode(null);
        IntegratorOutputNode hldgsNode = new IntegratorOutputNode(null);
        IntegratorOutputNode hldgNode = new IntegratorOutputNode(hldgSxf);
        IntegratorOutputNode itemsNode = new IntegratorOutputNode(null);
        IntegratorOutputNode itemNode = new IntegratorOutputNode(itemSxf);
        //IntegratorOutputNode itemStatusesNode = new IntegratorOutputNode(null);
        //IntegratorOutputNode itemStatusNode = new IntegratorOutputNode(itemStatusSxf);
        hldgsNode.addChild("hldg", hldgNode, false);
        recordNode.addChild("marc", new IntegratorOutputNode(marcSxf), false);
        recordNode.addChild("hldgs", hldgsNode, false);
        //hldgNode.addChild("items", itemsNode, false);
        //itemsNode.addChild("item", itemNode, false);
        //itemNode.addChild("itemStatuses", itemStatusesNode, false);
        //itemStatusesNode.addChild("itemStatus", itemStatusNode, false);
        rootOutputNode.addChild("record", recordNode, false);
        boolean raw = false;
        if (!raw) {
            t.transform(new SAXSource(rootOutputNode, new InputSource()), new StreamResult("/tmp/blah.xml"));
        } else {
            BufferingXMLFilter rawOutput = new BufferingXMLFilter();
            rootOutputNode.setContentHandler(rawOutput);
            try {
                rootOutputNode.run();
            } catch (Exception e) {
                e.printStackTrace(System.out);
            }
            rawOutput.play(null);
        }
    }
    public static final String TRANSFORMER_FACTORY_CLASS_NAME = "net.sf.saxon.TransformerFactoryImpl";
}
