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

import edu.upennlib.xmlutils.UnboundedContentHandlerBuffer;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.Source;
import javax.xml.transform.Templates;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import net.sf.saxon.Controller;
import org.apache.log4j.Logger;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

/**
 *
 * @author Michael Gibney
 */
public class TXMLFilter extends JoiningXMLFilter {

    private final ProcessingQueue<Chunk> pq;
    private final Templates templates;
    private final SplittingXMLFilter splitter = new SplittingXMLFilter();
    private static final Logger logger = Logger.getLogger(TXMLFilter.class);
    private ExecutorService executor;

    public TXMLFilter(Source xslSource) throws TransformerConfigurationException {
        TransformerFactory tf = TransformerFactory.newInstance("net.sf.saxon.TransformerFactoryImpl", null);
        templates = tf.newTemplates(xslSource);
        pq = new ProcessingQueue<Chunk>(100, new Chunk(templates));
    }
    
    public static void main(String[] args) throws TransformerConfigurationException, SAXException, ParserConfigurationException, FileNotFoundException, TransformerException {
        File in = new File(args[0]);
        File xsl = new File(args[1]);
        File out = new File(args[2]);
        TXMLFilter txf = new TXMLFilter(new StreamSource(xsl));
        SAXParserFactory spf = SAXParserFactory.newInstance();
        spf.setNamespaceAware(true);
        txf.setParent(spf.newSAXParser().getXMLReader());
        TransformerFactory tf = TransformerFactory.newInstance("net.sf.saxon.TransformerFactoryImpl", null);
        Transformer t = tf.newTransformer();
        txf.configureOutputTransformer((Controller) t);
        t.transform(new SAXSource(txf, new InputSource(new FileInputStream(in))), new StreamResult(out));
    }

    private XMLReader externalParent;

    public ExecutorService getExecutor() {
        return executor;
    }

    public void setExecutor(ExecutorService executor) {
        this.executor = executor;
    }

    @Override
    public XMLReader getParent() {
        return externalParent;
    }

    @Override
    public void setParent(XMLReader parent) {
        externalParent = parent;
        super.setParent(parent);
    }

    @Override
    public void parse(InputSource input) throws SAXException, IOException {
        executor.submit(new ProducerCallable(input, Thread.currentThread()));
        while (!pq.isFinished()) {
            try {
                Chunk nextOut = pq.nextOut();
                nextOut.writeOutputTo(TXMLFilter.this);
                nextOut.setState(ProcessingState.READY);
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            }
        }
        finished();
    }


    private class ProducerCallable implements Callable<Void> {

        private final InputSource input;
        private final Thread parentThread;

        private ProducerCallable(InputSource input, Thread parentThread) {
            this.input = input;
            this.parentThread = parentThread;
        }

        @Override
        public Void call() throws SAXException, IOException {
            Thread currentThread = Thread.currentThread();
            currentThread.setUncaughtExceptionHandler(new TargetedUncaughtExceptionHandler(currentThread.getUncaughtExceptionHandler(), parentThread));
            TXMLFilter.super.setParent(splitter);
            splitter.setParent(externalParent);
            splitter.setDTDHandler(TXMLFilter.this);
            splitter.setEntityResolver(TXMLFilter.this);
            splitter.setXMLReaderCallback(callback);
            TXMLFilter.super.parse(input);
            pq.finished();
            return null;
        }

    }

    private static class TargetedUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {

        private final Thread.UncaughtExceptionHandler restore;
        private final Thread target;

        private TargetedUncaughtExceptionHandler(Thread.UncaughtExceptionHandler restore, Thread target) {
            this.restore = restore;
            this.target = target;
        }

        @Override
        public void uncaughtException(Thread t, Throwable e) {
            target.interrupt();
            t.setUncaughtExceptionHandler(restore);
        }

    }

    @Override
    public void parse(String systemId) throws SAXException, IOException {
        throw new UnsupportedOperationException();
    }

    private final MyXMLReaderCallback callback = new MyXMLReaderCallback();

    private class MyXMLReaderCallback implements SplittingXMLFilter.XMLReaderCallback {

        @Override
        public void callback(XMLReader reader, InputSource input) throws SAXException, IOException {
            doCallback(reader, input, null);
        }

        @Override
        public void callback(XMLReader reader, String systemId) throws SAXException, IOException {
            doCallback(reader, null, systemId);
        }

        private void doCallback(XMLReader reader, InputSource input, String systemId) throws SAXException, IOException {
            Chunk nextIn;
            try {
                nextIn = pq.nextIn();
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            }
            UnboundedContentHandlerBuffer inputBuffer = nextIn.getInput();
            reader.setContentHandler(inputBuffer);
            reader.setErrorHandler(inputBuffer);
            if (input != null) {
                reader.parse(input);
            } else {
                reader.parse(systemId);
            }
            nextIn.setState(ProcessingState.HAS_INPUT);
        }

    }

    public void configureOutputTransformer(Controller out) {
        if (templates != null) {
            try {
                Controller c;
                c = (Controller) templates.newTransformer();
                out.getExecutable().setCharacterMapIndex(c.getExecutable().getCharacterMapIndex());
                out.setOutputProperties(c.getOutputProperties());

            } catch (TransformerConfigurationException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

}
