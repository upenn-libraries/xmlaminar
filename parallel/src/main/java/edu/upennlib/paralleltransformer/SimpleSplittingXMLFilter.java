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
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.Executors;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

/**
 *
 * @author Michael Gibney
 */
public class SimpleSplittingXMLFilter extends SplittingXMLFilter {

    private static final int DEFAULT_RECORD_LEVEL = 1;
    private static final int DEFAULT_CHUNK_SIZE = 100;
    private int chunkSize = DEFAULT_CHUNK_SIZE;
    private int level = -1;
    private int recordStartEvent = -1;
    private int recordCount = 0;

    public static void main(String[] args) throws Exception {
        TransformerFactory tf = TransformerFactory.newInstance("net.sf.saxon.TransformerFactoryImpl", null);
        Transformer t = tf.newTransformer();
        SimpleSplittingXMLFilter sxf = new SimpleSplittingXMLFilter();
        sxf.setInputType(InputType.indirect);
        sxf.setChunkSize(1);
        SAXParserFactory spf = SAXParserFactory.newInstance();
        spf.setNamespaceAware(true);
        SAXParser sp = spf.newSAXParser();
        XMLReader xmlReader = sp.getXMLReader();
        sxf.setParent(xmlReader);
        sxf.setXMLReaderCallback(new IncrementingFileCallback(0, t, "out/out-", ".xml"));
        File in = new File("blah.txt");
        InputSource inSource = new InputSource(new FileReader(in));
        inSource.setSystemId(in.getPath());
        sxf.setExecutor(Executors.newCachedThreadPool());
        try {
            sxf.parse(inSource);
        } finally {
            sxf.shutdown();
        }
    }

    public int getChunkSize() {
        return chunkSize;
    }

    public void setChunkSize(int size) {
        chunkSize = size;
    }

    @Override
    protected void reset(boolean cancel) {
        recordCount = 0;
        level = -1;
        recordStartEvent = -1;
        super.reset(cancel);
    }

    private void recordStart() throws SAXException {
        if (++recordCount > chunkSize) {
            recordCount = 1; // the record we just entered!
            splitStart();
        }
    }

    @Override
    public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
        if (++level == DEFAULT_RECORD_LEVEL && recordStartEvent++ < 0) {
            recordStart();
        }
        super.startElement(uri, localName, qName, atts);
    }

    @Override
    public void startPrefixMapping(String prefix, String uri) throws SAXException {
        if (level == DEFAULT_RECORD_LEVEL && recordStartEvent++ < 0) {
            recordStart();
        }
        super.startPrefixMapping(prefix, uri);
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        super.endElement(uri, localName, qName);
        if (--level == DEFAULT_RECORD_LEVEL && --recordStartEvent < 0) {
            splitEnd();
        }
    }

    @Override
    public void endPrefixMapping(String prefix) throws SAXException {
        if (level == DEFAULT_RECORD_LEVEL && --recordStartEvent < 0) {
            splitEnd();
        }
        super.endPrefixMapping(prefix);
    }
}
