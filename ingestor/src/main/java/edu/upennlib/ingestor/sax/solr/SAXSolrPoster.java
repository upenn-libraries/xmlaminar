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

package edu.upennlib.ingestor.sax.solr;

import edu.upennlib.dla.solrIndex.indexers.ShardedIndexer;
import edu.upennlib.ingestor.sax.xsl.SplittingXMLFilter;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;
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
public class SAXSolrPoster implements Runnable {

    private final SplittingXMLFilter splitter;
    private final Transformer t;
    private InputStream input;
    private ShardedIndexer indexer;

    public SAXSolrPoster() {
        splitter = new SplittingXMLFilter();
        SAXParserFactory spf = SAXParserFactory.newInstance();
        spf.setNamespaceAware(true);
        try {
            splitter.setParent(spf.newSAXParser().getXMLReader());
        } catch (ParserConfigurationException ex) {
            throw new RuntimeException(ex);
        } catch (SAXException ex) {
            throw new RuntimeException(ex);
        }
        try {
            t = TransformerFactory.newInstance().newTransformer();
        } catch (TransformerConfigurationException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void run() {
        InputSource inputSource = new InputSource(input);
        Thread indexerAdderThread = new Thread(new IndexerAdder(), "indexerAdderThread");
        indexerAdderThread.start();
        try {
            do {
                PipedOutputStream pos = new PipedOutputStream();
                PipedInputStream pis = new PipedInputStream(pos);
                BufferedOutputStream bos = new BufferedOutputStream(pos);
                BufferedInputStream bis = new BufferedInputStream(pis);
                synchronized(inStream) {
                    while (inStream[0] != null) {
                        inStream.wait();
                    }
                    inStream[0] = bis;
                    inStream.notify();
                }
                t.transform(new SAXSource(splitter, inputSource), new StreamResult(bos));
                bos.close();
            } while (splitter.hasMoreOutput(inputSource));
            finished = true;
            synchronized(inStream) {
                inStream.notifyAll();
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        } catch (TransformerException ex) {
            throw new RuntimeException(ex);
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
    }

    private final InputStream[] inStream = new InputStream[1];
    private boolean finished = false;

    private class IndexerAdder implements Runnable {

        @Override
        public void run() {
            synchronized (inStream) {
                while (!finished) {
                    if (inStream[0] == null) {
                        try {
                            inStream.wait();
                        } catch (InterruptedException ex) {
                            throw new RuntimeException(ex);
                        }
                    } else {
                        indexer.add(inStream[0]);
                        inStream[0] = null;
                        inStream.notify();
                    }
                }
            }
        }

    }

    public void setInput(InputStream input) {
        this.input = input;
    }

    public void setIndexer(ShardedIndexer indexer) {
        this.indexer = indexer;
    }

}
