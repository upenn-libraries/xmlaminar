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
import java.io.FileReader;
import java.util.concurrent.Executors;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

/**
 *
 * @author Michael Gibney
 */
public class LevelSplittingXMLFilter extends SplittingXMLFilter {

    private static final int DEFAULT_RECORD_LEVEL = 1;
    private static final int DEFAULT_CHUNK_SIZE = 100;
    private final LevelSplitDirector levelSplitDirector;
    private int recordLevel;
    private int chunkSize;
    private int recordCount;

    public LevelSplittingXMLFilter() {
        this(DEFAULT_RECORD_LEVEL, DEFAULT_CHUNK_SIZE);
    }
    
    public LevelSplittingXMLFilter(int splitLevel, int chunkSize) {
        this.recordLevel = splitLevel;
        this.chunkSize = chunkSize;
        this.levelSplitDirector = new LevelSplitDirector();
        super.setSplitDirector(levelSplitDirector);
    }
    
    public static void main(String[] args) throws Exception {
        TransformerFactory tf = TransformerFactory.newInstance("net.sf.saxon.TransformerFactoryImpl", null);
        Transformer t = tf.newTransformer();
        LevelSplittingXMLFilter sxf = new LevelSplittingXMLFilter();
        sxf.setInputType(InputType.direct);
        sxf.setChunkSize(1);
        SAXParserFactory spf = SAXParserFactory.newInstance();
        spf.setNamespaceAware(true);
        SAXParser sp = spf.newSAXParser();
        XMLReader xmlReader = sp.getXMLReader();
        sxf.setParent(xmlReader);
        sxf.setOutputCallback(new IncrementingFileCallback(0, t, "out/out-", ".xml"));
        File in = new File("blah.xml");
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
        if (size < 1) {
            throw new IllegalArgumentException("chunk size "+size+" < 1");
        }
        this.chunkSize = size;
    }

    public int getRecordLevel() {
        return recordLevel;
    }

    public void setRecordLevel(int level) {
        this.recordLevel = level;
    }
    
    public void restoreLevelSplitDirector() {
        super.setSplitDirector(levelSplitDirector);
    }

    private class LevelSplitDirector extends SplitDirector {

        @Override
        public void reset() {
            recordCount = 0;
        }
        
        @Override
        public SplitDirective startElement(String uri, String localName, String qName, Attributes atts, int level) throws SAXException {
            if (level == recordLevel) {
                if (recordCount++ >= chunkSize) {
                    recordCount = 1; // the one we just entered
                    return SplitDirective.SPLIT;
                } else {
                    return SplitDirective.NO_SPLIT_BYPASS;
                }
            } else {
                return SplitDirective.NO_SPLIT;
            }
        }
    }
}
