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
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 *
 * @author michael
 */
public class IntegratorSAX implements Runnable {
    private StatefulXMLFilter[] sources;
    private final IntegratorOutputNode rootOutputNode = new IntegratorOutputNode(null);

    private void blockUntilAtRest() {
        for (StatefulXMLFilter sxf : sources) {
            sxf.blockUntilAtRest();
        }
    }

    private Comparable evaluateNextLeastId() {
        int leastIdSourceLevel = -1;
        Comparable leastId = null;
        for (int i = 0; i < sources.length; i++) {
            int level = sources[i].getLevel();
            if (level > leastIdSourceLevel) {
                leastIdSourceLevel = level;
                leastId = sources[i].getId();
            } else if (level == leastIdSourceLevel && sources[i].getId().compareTo(leastId) < 0) {
                leastId = sources[i].getId();
            }
        }
        return leastId;
    }

    private void wakeUpdatedSources() {
        for (StatefulXMLFilter sxf : sources) {
            if (sxf.isUpdated()) {
                synchronized(sxf) {
                    sxf.notify();
                }
            }
        }
    }

    ContentHandler ch;

    @Override
    public void run() {
        while (true) {
            blockUntilAtRest();
            if (rootOutputNode.isWritable()) {
                try {
                    rootOutputNode.writeOutput(ch);
                } catch (SAXException ex) {
                    throw new RuntimeException(ex);
                }
            }
            Comparable leastId = evaluateNextLeastId();
            rootOutputNode.prepare(leastId);
            wakeUpdatedSources();
        }
    }

    public static void main(String[] args) throws ParserConfigurationException, SAXException, FileNotFoundException, IOException {
        IntegratorSAX instance = new IntegratorSAX();
        instance.setupAndRun();
    }

    private void setupAndRun() throws ParserConfigurationException, SAXException, FileNotFoundException {
        File marcFile = new File("inputFiles/integrator_xml/marc.xml");
        File hldgFile = new File("inputFiles/integrator_xml/mfhd.xml");
        File itemFile = new File("inputFiles/integrator_xml/item.xml");
        File itemStatusFile = new File("inputFiles/integrator_xml/item_status.xml");
        StatefulXMLFilter marcSxf = new StatefulXMLFilter();
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
        }
        ArrayList<StatefulXMLFilter> sxfs = new ArrayList<StatefulXMLFilter>();
        sxfs.add(marcSxf);
        sources = sxfs.toArray(new StatefulXMLFilter[0]);
        rootOutputNode.addChild("marc", new IntegratorOutputNode(marcSxf));
        rootOutputNode.run();
    }

}
