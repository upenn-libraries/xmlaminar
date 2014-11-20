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

import edu.upennlib.paralleltransformer.callback.IncrementingFileCallback;
import edu.upennlib.paralleltransformer.callback.QueueDestCallback;
import edu.upennlib.paralleltransformer.callback.XMLReaderCallback;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
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
import javax.xml.transform.stream.StreamSource;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

/**
 *
 * @author Michael Gibney
 */
public class NewClass {

    public static void main(String[] args) throws Exception {
        mainSplitTransformJoin(args);
    }
    
    public static void mainSplitTransform(String[] args) throws Exception {
        args = new String[] {"blah.txt", "identity.xsl", "out/out"};
        File in = new File(args[0]);
        File xsl = new File(args[1]);
        File out = new File(args[2]);
        final TXMLFilter1 txf = new TXMLFilter1(new StreamSource(xsl));
        txf.setInputType(QueueSourceXMLFilter.InputType.queue);
        LevelSplittingXMLFilter sxf = new LevelSplittingXMLFilter();
        sxf.setInputType(QueueSourceXMLFilter.InputType.indirect);
        sxf.setChunkSize(1);
        SAXParserFactory spf = SAXParserFactory.newInstance();
        spf.setNamespaceAware(true);
        SAXParser sp = spf.newSAXParser();
        XMLReader xmlReader = sp.getXMLReader();
        txf.setParent(sxf);
        sxf.setParent(xmlReader);
        sxf.setOutputCallback(new XMLReaderCallback() {
            int i = 0;
            @Override
            public void callback(XMLReader reader, InputSource input) throws SAXException, IOException {
                try {
                    txf.getParseQueue().put(new SAXSource(reader, input));
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
                    txf.getParseQueue().put(QueueSourceXMLFilter.FINISHED);
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
            }
        });
        txf.setOutputCallback(new IncrementingFileCallback("out/out"));
        InputSource inSource = new InputSource(new FileReader(in));
        inSource.setSystemId(in.getPath());
        ExecutorService executor = Executors.newCachedThreadPool();
        sxf.setExecutor(executor);
        txf.setExecutor(executor);
        try {
            txf.parse(inSource);
        } finally {
            executor.shutdown();
        }
    }
    
    public static void mainSplitTransformJoin(String[] args) throws Exception {
        args = new String[] {"blah.txt", "identity.xsl", "out.xml"};
        File in = new File(args[0]);
        File xsl = new File(args[1]);
        File out = new File(args[2]);
        TXMLFilter1 txf = new TXMLFilter1(new StreamSource(xsl));
        txf.setInputType(QueueSourceXMLFilter.InputType.queue);
        LevelSplittingXMLFilter sxf = new LevelSplittingXMLFilter();
        sxf.setInputType(QueueSourceXMLFilter.InputType.indirect);
        sxf.setChunkSize(1);
        JoiningXMLFilter joiner = new JoiningXMLFilter();
        joiner.setInputType(QueueSourceXMLFilter.InputType.queue);
        SAXParserFactory spf = SAXParserFactory.newInstance();
        spf.setNamespaceAware(true);
        SAXParser sp = spf.newSAXParser();
        XMLReader xmlReader = sp.getXMLReader();
        joiner.setParent(txf);
        txf.setParent(sxf);
        sxf.setParent(xmlReader);
        sxf.setOutputCallback(new QueueDestCallback(txf));
        txf.setOutputCallback(new QueueDestCallback(joiner));
        InputSource inSource = new InputSource(new FileReader(in));
        inSource.setSystemId(in.getPath());
        ExecutorService executor = Executors.newCachedThreadPool();
        sxf.setExecutor(executor);
        txf.setExecutor(executor);
        joiner.setExecutor(executor);
        TransformerFactory tf = TransformerFactory.newInstance("net.sf.saxon.TransformerFactoryImpl", null);
        Transformer t = tf.newTransformer();
        try {
            t.transform(new SAXSource(joiner, inSource), new StreamResult(out));
        } finally {
            executor.shutdown();
        }
    }
    
    public static void mainSplitJoin(String[] args) throws InterruptedException, ParserConfigurationException, SAXException, FileNotFoundException, IOException, TransformerConfigurationException, TransformerException {
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
        joiner.setParent(sxf);
        sxf.setParent(xmlReader);
        sxf.setOutputCallback(new XMLReaderCallback() {
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
    
    public static void mainTXMLFilter(String[] args) throws Exception {
        args = new String[] {"blah.txt", "identity.xsl", "out.xml"};
        File in = new File(args[0]);
        File xsl = new File(args[1]);
        File out = new File(args[2]);
        TXMLFilter1 txf = new TXMLFilter1(new StreamSource(xsl));
        SAXParserFactory spf = SAXParserFactory.newInstance();
        spf.setNamespaceAware(true);
        txf.setParent(spf.newSAXParser().getXMLReader());
        txf.setInputType(QueueSourceXMLFilter.InputType.indirect);
        txf.setOutputCallback(new IncrementingFileCallback("out"));
        ExecutorService executor = Executors.newCachedThreadPool();
        txf.setExecutor(executor);
        txf.parse(new InputSource(new FileInputStream(in)));
        executor.shutdown();
    }
}
