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

package edu.upennlib.ingestor.sax.xsl;

import edu.upennlib.ingestor.sax.utils.MyXFI;
import edu.upennlib.ingestor.sax.utils.NoopXMLFilter;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashSet;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.Result;
import javax.xml.transform.Templates;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.sax.SAXResult;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.sax.SAXTransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import net.sf.saxon.Controller;
import net.sf.saxon.serialize.CharacterMapIndex;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXNotSupportedException;
import org.xml.sax.XMLReader;
import org.xml.sax.ext.LexicalHandler;

/**
 *
 * @author michael
 */
public class JoiningXMLFilter extends MyXFI {

    public final int RECORD_LEVEL = 1;
    private int level = -1;
    private boolean encounteredFirstRecord = false;
    private boolean inPreRecord = true;
    public static final String TRANSFORMER_FACTORY_CLASS_NAME = "net.sf.saxon.TransformerFactoryImpl";
    private static final SAXTransformerFactory tf = (SAXTransformerFactory)TransformerFactory.newInstance(TRANSFORMER_FACTORY_CLASS_NAME, null);
    public static final int TRANSFORMER_THREAD_COUNT = 3;

    public static void main(String[] args) throws SAXException, ParserConfigurationException, FileNotFoundException, TransformerConfigurationException, TransformerException, IOException {
        File stylesheet = new File("inputFiles/franklin_nsaware.xsl");
        File inputFile = new File("inputFiles/largest.xml");
        File outputFile = new File("outputFiles/large_transform.xml");

        BufferedInputStream bis = new BufferedInputStream(new FileInputStream(inputFile));
        InputSource inputSource = new InputSource(bis);

        BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(outputFile));
        JoiningXMLFilter instance = new JoiningXMLFilter();

        long start = System.currentTimeMillis();
        instance.transform(inputSource, stylesheet, new StreamResult(bos));
        bos.close();
        System.out.println("duration: " + (System.currentTimeMillis() - start));
    }

    UnboundedContentHandlerBuffer docLevelEventBuffer = new UnboundedContentHandlerBuffer();
    SaxEventVerifier preRecordEventVerifier = new SaxEventVerifier();
    File stylesheet;

    public void transform(InputSource source, File stylesheet, StreamResult result) throws ParserConfigurationException, SAXException, FileNotFoundException, TransformerConfigurationException, TransformerException {
        SAXParserFactory spf = SAXParserFactory.newInstance();
        spf.setNamespaceAware(true);
        SAXParser parser = spf.newSAXParser();
        XMLReader originalReader = parser.getXMLReader();

        SplittingXMLFilter sxf = new SplittingXMLFilter();
        sxf.setParent(originalReader);

        setParent(originalReader);
        setUpstreamSplittingFilter(sxf);
        setStylesheet(stylesheet);

        Controller mainController;
        synchronized (tf) {
            mainController = (Controller) tf.newTransformer();
        }
        synchronized (tf) {
            try {
                Templates th = tf.newTemplates(new StreamSource(stylesheet));
                Controller subControllerInstance = (Controller) th.newTransformer();
                mainController.getExecutable().setCharacterMapIndex(subControllerInstance.getExecutable().getCharacterMapIndex());
                mainController.setOutputProperties(subControllerInstance.getOutputProperties());
            } catch (TransformerConfigurationException ex) {
                throw new RuntimeException(ex);
            }
        }
        mainController.transform(new SAXSource(this, source), result);
    }


    public void transform(XMLReader source, File stylesheet, Result result) throws ParserConfigurationException, SAXException, FileNotFoundException, TransformerConfigurationException, TransformerException {
        SplittingXMLFilter sxf = new SplittingXMLFilter();
        sxf.setParent(source);

        //Dummy
        SAXParserFactory spf = SAXParserFactory.newInstance();
        spf.setNamespaceAware(true);

        setParent(spf.newSAXParser().getXMLReader());
        setUpstreamSplittingFilter(sxf);
        setStylesheet(stylesheet);

        Controller mainController;
        synchronized (tf) {
            mainController = (Controller) tf.newTransformer();
        }
        synchronized (tf) {
            try {
                Templates th = tf.newTemplates(new StreamSource(stylesheet));
                Controller subControllerInstance = (Controller) th.newTransformer();
                mainController.getExecutable().setCharacterMapIndex(subControllerInstance.getExecutable().getCharacterMapIndex());
                mainController.setOutputProperties(subControllerInstance.getOutputProperties());
            } catch (TransformerConfigurationException ex) {
                throw new RuntimeException(ex);
            }
        }
        mainController.transform(new SAXSource(this, new InputSource()), result);
    }

    public void setStylesheet(File stylesheet) {
        this.stylesheet = stylesheet;
    }

    public void setUpstreamSplittingFilter(SplittingXMLFilter splitter) {
        this.splitter[0] = splitter;
    }

    private final SplittingXMLFilter[] splitter = new SplittingXMLFilter[1];
    private UnboundedContentHandlerBuffer inputEventBuffer = new UnboundedContentHandlerBuffer();

    @Override
    public void parse(InputSource input)
            throws SAXException, IOException {
//        NoopXMLFilter noop = new NoopXMLFilter();
//        noop.setParent(this);
//        inputEventBuffer.setParent(noop);
        if (splitter[0] != null) {
            splitter[0].setDTDHandler(this);
            splitter[0].setErrorHandler(this);
            splitter[0].setEntityResolver(this);
            transformerRunnerBuffer = new TransformerOutputBuffer(this, this, splitter[0], input);
            for (int i = 0; i< TRANSFORMER_THREAD_COUNT; i++) {
                Thread thread = new Thread(new TransformerRunner(transformerRunnerBuffer, input), "tr"+i);
                transformerRunnerPool.add(thread);
            }
            Thread outputThread = new Thread(new OutputThread(), "outputThread");
            for (Thread t : transformerRunnerPool) {
                t.start();
            }
            outputThread.start();
            try {
                for (Thread t : transformerRunnerPool) {
                    t.join();
                }
                outputThread.join();
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            }
            docLevelEventBuffer.play(getContentHandler());
        }
    }


    private HashSet<Thread> transformerRunnerPool = new HashSet<Thread>();
    private TransformerOutputBuffer transformerRunnerBuffer;

    private static class TransformerOutputBuffer {
        final int MAX_SIZE = TRANSFORMER_THREAD_COUNT;
        enum BufferState {FREE, CHECKED_OUT, WRITEABLE, EOF};
        int size = 0;
        int head = 0;
        int tail = 0;
        final UnboundedContentHandlerBuffer[] inputs = new UnboundedContentHandlerBuffer[MAX_SIZE];
        final UnboundedContentHandlerBuffer[] outputs = new UnboundedContentHandlerBuffer[MAX_SIZE];
        BufferState[] states = new BufferState[MAX_SIZE];
        final SplittingXMLFilter tobSplitter;
        final InputSource dummyInputSource;

        public TransformerOutputBuffer(XMLReader original, JoiningXMLFilter joiner, SplittingXMLFilter splitter, InputSource dummyInputSource) {
            tobSplitter = splitter;
            this.dummyInputSource = dummyInputSource;
            for (int i = 0; i < inputs.length; i++) {
                inputs[i] = new UnboundedContentHandlerBuffer();
                //inputs[i].setParent(original);
                inputs[i].setDTDHandler(joiner);
                inputs[i].setEntityResolver(joiner);
                inputs[i].setErrorHandler(joiner);
                outputs[i] = new UnboundedContentHandlerBuffer();
                states[i] = BufferState.FREE;
            }
        }

        private boolean parsingInitiated = false;
        public void checkOut(TransformerRunner tr) throws EOFException, SAXException, IOException {
            synchronized(inputs) {
                while (size >= MAX_SIZE || states[tail] != BufferState.FREE) {
                    try {
                        if (states[tail] == BufferState.EOF) {
                            throw new EOFException();
                        }
                        inputs.wait();
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                }
                inputs[tail].clear();
                outputs[tail].clear();
                if (parsingInitiated && !tobSplitter.hasMoreOutput(dummyInputSource)) {
                    throw new EOFException();
                }
                parsingInitiated = true;
                tobSplitter.setContentHandler(inputs[tail]);
                //tobSplitter.setProperty("http://xml.org/sax/properties/lexical-handler", inputs[tail]);
                tobSplitter.parse(dummyInputSource);
                tr.setIO(inputs[tail], outputs[tail], tail);
                states[tail] = BufferState.CHECKED_OUT;
                tail = ++tail % MAX_SIZE;
                size++;
            }
        }

        public void finishedTask(int index) {
            if (states[index] != BufferState.CHECKED_OUT) {
                throw new IllegalStateException("state is: "+states[index]);
            }
            states[index] = BufferState.WRITEABLE;
            synchronized(outputs) {
                outputs.notifyAll();
            }
        }

        public void writeTo(ContentHandler ch) throws SAXException, EOFException {
            synchronized (outputs) {
                while (states[head] != BufferState.WRITEABLE) {
                    try {
                        if (states[head] == BufferState.EOF) {
                            throw new EOFException();
                        }
                        if (allFinished && states[head] == BufferState.FREE) {
                            throw new EOFException();
                        }
                        outputs.wait();
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }
            outputs[head].flush(ch);
            inputs[head].clear();
            states[head] = BufferState.FREE;
            head = ++head % MAX_SIZE;
            size--;
            synchronized (inputs) {
                inputs.notify();
            }
        }

        boolean allFinished = false;

        public void inputFinished() {
            allFinished = true;
            synchronized(outputs) {
                outputs.notifyAll();
            }
        }

        public void cancelTask(int index) {
            synchronized(outputs) {
                allFinished = true;
                states[index] = BufferState.EOF;
                size--;
                outputs.notifyAll();
            }
            synchronized(inputs) {
                inputs.notifyAll();
            }
        }


    }

    private final CharacterMapIndex[] charMapIndex = new CharacterMapIndex[1];

    private class TransformerRunner implements Runnable {

        final TransformerOutputBuffer tob;
        final Transformer t;
        final InputSource dummyInputSource;
        final SplittingXMLFilter subSplitter;
        final UnboundedContentHandlerBuffer individualInputBuffer;
        UnboundedContentHandlerBuffer in;
        UnboundedContentHandlerBuffer out;
        UnboundedContentHandlerBuffer localOutputEventBuffer = new UnboundedContentHandlerBuffer();
        int index = -1;

        public TransformerRunner(TransformerOutputBuffer tob, InputSource dummyInputSource) {
            subSplitter = new SplittingXMLFilter();
            subSplitter.setChunkSize(1);
            subSplitter.setDTDHandler(JoiningXMLFilter.this);
            subSplitter.setEntityResolver(JoiningXMLFilter.this);
            subSplitter.setErrorHandler(JoiningXMLFilter.this);
            individualInputBuffer = new UnboundedContentHandlerBuffer();
            this.tob = tob;
            this.dummyInputSource = dummyInputSource;
            synchronized(tf) {
                try {
                    Templates th = tf.newTemplates(new StreamSource(stylesheet));
                    t = th.newTransformer();
                } catch (TransformerConfigurationException ex) {
                    throw new RuntimeException(ex);
                }
            }
        }

        @Override
        public void run() {
            try {
                do {
                    try {
                        try {
                            synchronized (tob) {
                                tob.checkOut(this);
                            }
                            SAXResult result = new SAXResult(out);
                            //result.setLexicalHandler(out);
                            t.transform(new SAXSource(in, new InputSource()), result);
                        } catch (TransformerException ex) {
                            subdivide(t, in, out, dummyInputSource);
                        } catch (SAXException ex) {
                            subdivide(t, in, out, dummyInputSource);
                        }
                    } catch (EOFException ex) {
                        throw ex;
                    } catch (IOException ex) {
                        throw new RuntimeException(ex);
                    }
                    finishedTask();
                } while (splitter[0].hasMoreOutput(dummyInputSource));
                tob.inputFinished();
            } catch (EOFException ex) {
                if (index != -1) {
                    tob.cancelTask(index);
                }
            }
        }

        private void finishedTask() {
            tob.finishedTask(index);
            in = null;
            out = null;
            index = -1;
        }

        public void setIO(UnboundedContentHandlerBuffer in, UnboundedContentHandlerBuffer out, int index) {
            this.in = in;
            this.out = out;
            this.index = index;
        }

        private void subdivide(Transformer t, UnboundedContentHandlerBuffer in, UnboundedContentHandlerBuffer out, InputSource dummyInput) throws IOException {
            out.clear();
            individualInputBuffer.clear();
            subSplitter.setParent(in);
            subSplitter.setContentHandler(individualInputBuffer);
            if (individualInputBuffer instanceof LexicalHandler) {
                try {
                    subSplitter.setProperty("http://xml.org/sax/properties/lexical-handler", individualInputBuffer);
                } catch (SAXNotRecognizedException ex) {
                    throw new RuntimeException(ex);
                } catch (SAXNotSupportedException ex) {
                    throw new RuntimeException(ex);
                }
            }
            do {
                try {
                    subSplitter.parse(dummyInput);
                    t.transform(new SAXSource(individualInputBuffer, new InputSource()), new SAXResult(localOutputEventBuffer));
                    localOutputEventBuffer.flush(out);
                } catch (SAXException ex) {
                    localOutputEventBuffer.clear();
                } catch (TransformerException ex) {
                    localOutputEventBuffer.clear();
                }
            } while (subSplitter.hasMoreOutput(dummyInput));
        }
    }

    private class OutputThread implements Runnable {
        
        @Override
        public void run() {
            try {
                while (true) {
                    transformerRunnerBuffer.writeTo(JoiningXMLFilter.this);
                }
            } catch (SAXException ex) {
                throw new RuntimeException(ex);
            } catch (EOFException ex) {
                // OK
            }
        }

    }

    @Override
    public void parse(String systemId) throws SAXException, IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void startDocument() throws SAXException {
        inPreRecord = true;
        docLevelEventBuffer.clear();
        if (!encounteredFirstRecord) {
            preRecordEventVerifier.recordStart();
            super.startDocument();
        } else {
            preRecordEventVerifier.verifyStart();
            docLevelEventBuffer.startDocument();
        }
    }

    @Override
    public void endDocument() throws SAXException {
        docLevelEventBuffer.endDocument();
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        if (level < RECORD_LEVEL) {
            if (inPreRecord) {
                preRecordEventVerifier.endElement(uri, localName, qName);
            }
            if (!encounteredFirstRecord) {
                super.endElement(uri, localName, qName);
            } else {
                docLevelEventBuffer.endElement(uri, localName, qName);
            }
        } else {
            if (level == RECORD_LEVEL) {
                docLevelEventBuffer.clear();
            }
            super.endElement(uri, localName, qName);
        }
        level--;
    }

    @Override
    public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
        level++;
        if (level < RECORD_LEVEL) {
            if (inPreRecord) {
                preRecordEventVerifier.startElement(uri, localName, qName, atts);
            }
            if (!encounteredFirstRecord) {
                super.startElement(uri, localName, qName, atts);
            } else {
                docLevelEventBuffer.startElement(uri, localName, qName, atts);
            }
        } else {
            if (level == RECORD_LEVEL) {
                if (!encounteredFirstRecord) {
                    preRecordEventVerifier.recordEnd();
                    encounteredFirstRecord = true;
                } else {
                    preRecordEventVerifier.verifyEnd();
                    if (!inPreRecord) {
                        level += docLevelEventBuffer.play(getContentHandler());
                    } else {
                        level += docLevelEventBuffer.playMostRecentStructurallyInsignificant(getContentHandler());
                    }
                }
                inPreRecord = false;
            }
            super.startElement(uri, localName, qName, atts);
        }
    }

    @Override
    public void setDocumentLocator(Locator locator) {
        docLevelEventBuffer.clear();
        super.setDocumentLocator(locator);
    }

    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        if (level < RECORD_LEVEL && encounteredFirstRecord) {
            docLevelEventBuffer.characters(ch, start, length);
        } else {
            super.characters(ch, start, length);
        }
    }

    @Override
    public void endPrefixMapping(String prefix) throws SAXException {
        if (level < RECORD_LEVEL) {
            if (inPreRecord) {
                preRecordEventVerifier.endPrefixMapping(prefix);
            }
            if (!encounteredFirstRecord) {
                super.endPrefixMapping(prefix);
            } else {
                docLevelEventBuffer.endPrefixMapping(prefix);
            }
        } else {
            super.endPrefixMapping(prefix);
        }
    }

//    @Override
//    public void error(SAXParseException e) throws SAXException {
//        System.out.println("error1");
//        if (level < RECORD_LEVEL && encounteredFirstRecord) {
//            docLevelEventBuffer.error(e);
//        } else {
//            super.error(e);
//        }
//    }
//
//    @Override
//    public void fatalError(SAXParseException e) throws SAXException {
//        System.out.println("error2");
//        if (level < RECORD_LEVEL && encounteredFirstRecord) {
//            docLevelEventBuffer.fatalError(e);
//        } else {
//            super.fatalError(e);
//        }
//    }
//
    @Override
    public void ignorableWhitespace(char[] ch, int start, int length) throws SAXException {
        if (level < RECORD_LEVEL && encounteredFirstRecord) {
            docLevelEventBuffer.ignorableWhitespace(ch, start, length);
        } else {
            super.ignorableWhitespace(ch, start, length);
        }
    }

//    @Override
//    public void notationDecl(String name, String publicId, String systemId) throws SAXException {
//        if (level < RECORD_LEVEL && encounteredFirstRecord) {
//            docLevelEventBuffer.notationDecl(name, publicId, systemId);
//        } else {
//            super.notationDecl(name, publicId, systemId);
//        }
//    }
//
    @Override
    public void processingInstruction(String target, String data) throws SAXException {
        if (level < RECORD_LEVEL) {
            if (inPreRecord) {
                preRecordEventVerifier.processingInstruction(target, data);
            }
            if (!encounteredFirstRecord) {
                super.processingInstruction(target, data);
            } else {
                docLevelEventBuffer.processingInstruction(target, data);
            }
        } else {
            super.processingInstruction(target, data);
        }
    }

    @Override
    public void skippedEntity(String name) throws SAXException {
        if (level < RECORD_LEVEL && encounteredFirstRecord) {
            docLevelEventBuffer.skippedEntity(name);
        } else {
            super.skippedEntity(name);
        }
    }

    @Override
    public void startPrefixMapping(String prefix, String uri) throws SAXException {
        if (level < RECORD_LEVEL) {
            if (inPreRecord) {
                preRecordEventVerifier.startPrefixMapping(prefix, uri);
            }
            if (!encounteredFirstRecord) {
                super.startPrefixMapping(prefix, uri);
            } else {
                docLevelEventBuffer.startPrefixMapping(prefix, uri);
            }
        } else {
            super.startPrefixMapping(prefix, uri);
        }
    }

//    @Override
//    public void unparsedEntityDecl(String name, String publicId, String systemId, String notationName) throws SAXException {
//        if (level < RECORD_LEVEL && encounteredFirstRecord) {
//            docLevelEventBuffer.unparsedEntityDecl(name, publicId, systemId, notationName);
//        } else {
//            super.unparsedEntityDecl(name, publicId, systemId, notationName);
//        }
//    }
//
//    @Override
//    public void warning(SAXParseException e) throws SAXException {
//        if (level < RECORD_LEVEL && encounteredFirstRecord) {
//            docLevelEventBuffer.warning(e);
//        } else {
//            super.warning(e);
//        }
//    }
//
//    @Override
//    public void comment(char[] ch, int start, int length) throws SAXException {
//        if (level < RECORD_LEVEL && encounteredFirstRecord) {
//            docLevelEventBuffer.comment(ch, start, length);
//        } else {
//            super.comment(ch, start, length);
//        }
//    }
//
//    @Override
//    public void endCDATA() throws SAXException {
//        if (level < RECORD_LEVEL && encounteredFirstRecord) {
//            docLevelEventBuffer.endCDATA();
//        } else {
//            super.endCDATA();
//        }
//    }
//
//    @Override
//    public void endDTD() throws SAXException {
//        if (level < RECORD_LEVEL && encounteredFirstRecord) {
//            docLevelEventBuffer.endDTD();
//        } else {
//            super.endDTD();
//        }
//    }
//
//    @Override
//    public void endEntity(String name) throws SAXException {
//        if (level < RECORD_LEVEL && encounteredFirstRecord) {
//            docLevelEventBuffer.endEntity(name);
//        } else {
//            super.endEntity(name);
//        }
//    }
//
//    @Override
//    public void startCDATA() throws SAXException {
//        if (level < RECORD_LEVEL && encounteredFirstRecord) {
//            docLevelEventBuffer.startCDATA();
//        } else {
//            super.startCDATA();
//        }
//    }
//
//    @Override
//    public void startDTD(String name, String publicId, String systemId) throws SAXException {
//        if (level < RECORD_LEVEL && encounteredFirstRecord) {
//            docLevelEventBuffer.startDTD(name, publicId, systemId);
//        } else {
//            super.startDTD(name, publicId, systemId);
//        }
//    }
//
//    @Override
//    public void startEntity(String name) throws SAXException {
//        if (level < RECORD_LEVEL && encounteredFirstRecord) {
//            docLevelEventBuffer.startEntity(name);
//        } else {
//            super.startEntity(name);
//        }
//    }
//


}
