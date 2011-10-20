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
import edu.upennlib.ingestor.sax.xsl.TransformerOutputBuffer.TransformerRunner;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.Result;
import javax.xml.transform.Templates;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.sax.SAXTransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import net.sf.saxon.Controller;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

/**
 *
 * @author michael
 */
public class JoiningXMLFilterThreadPool extends MyXFI {

    public final int RECORD_LEVEL = 1;
    private int level = -1;
    private boolean encounteredFirstRecord = false;
    private boolean inPreRecord = true;
    public static final String TRANSFORMER_FACTORY_CLASS_NAME = "net.sf.saxon.TransformerFactoryImpl";
    private static final SAXTransformerFactory tf = (SAXTransformerFactory)TransformerFactory.newInstance(TRANSFORMER_FACTORY_CLASS_NAME, null);

    public static void main(String[] args) throws SAXException, ParserConfigurationException, FileNotFoundException, TransformerConfigurationException, TransformerException, IOException {
        File stylesheet = new File("inputFiles/franklin_nsaware.xsl");
        File inputFile = new File("inputFiles/largest.xml");
        File outputFile = new File("/tmp/large_transform.xml");

        BufferedInputStream bis = new BufferedInputStream(new FileInputStream(inputFile));
        InputSource inputSource = new InputSource(bis);

        BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(outputFile));
        JoiningXMLFilterThreadPool instance = new JoiningXMLFilterThreadPool();

        long start = System.currentTimeMillis();
        instance.transform(inputSource, stylesheet, new StreamResult(bos));
        bos.close();
        System.out.println("duration: " + (System.currentTimeMillis() - start));
    }

    private final UnboundedContentHandlerBuffer docLevelEventBuffer = new UnboundedContentHandlerBuffer();
    private final SaxEventVerifier preRecordEventVerifier = new SaxEventVerifier();
    private File stylesheet = null;

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


    public void transform(XMLReader sourceReader, InputSource inputSource, File stylesheet, Result result) throws ParserConfigurationException, SAXException, FileNotFoundException, TransformerConfigurationException, TransformerException {
        SplittingXMLFilter sxf = new SplittingXMLFilter();
        sxf.setParent(sourceReader);

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
                Controller subControllerInstance;
                if (stylesheet != null) {
                    Templates th = tf.newTemplates(new StreamSource(stylesheet));
                    subControllerInstance = (Controller) th.newTransformer();
                } else {
                    subControllerInstance = (Controller) tf.newTransformer();
                }
                mainController.getExecutable().setCharacterMapIndex(subControllerInstance.getExecutable().getCharacterMapIndex());
                mainController.setOutputProperties(subControllerInstance.getOutputProperties());
            } catch (TransformerConfigurationException ex) {
                throw new RuntimeException(ex);
            }
        }
        mainController.transform(new SAXSource(this, inputSource), result);
    }

    private Templates stylesheetTemplates;

    public void setStylesheet(File stylesheet) throws TransformerConfigurationException {
        if (stylesheet == null) {
            stylesheetTemplates = null;
        } else {
            stylesheetTemplates = tf.newTemplates(new StreamSource(stylesheet));
        }
        this.stylesheet = stylesheet;
    }

    public void setUpstreamSplittingFilter(SplittingXMLFilter splitter) {
        this.splitter = splitter;
    }

    private SplittingXMLFilter splitter;

    @Override
    public void parse(InputSource input)
            throws SAXException, IOException {
        if (splitter != null) {
            int BUFFER_SIZE = 10;
            splitter.setDTDHandler(this);
            splitter.setErrorHandler(this);
            splitter.setEntityResolver(this);
            TransformerOutputBuffer tobnew;
            try {
                tobnew = new TransformerOutputBuffer(BUFFER_SIZE, this, stylesheetTemplates);
            } catch (TransformerConfigurationException ex) {
                throw new RuntimeException(ex);
            }
            ExecutorService executor = Executors.newCachedThreadPool();
            Thread outputThread = new Thread(tobnew, "outputThread");
            outputThread.start();
            long n = 0;
            do {
                TransformerRunner tr = tobnew.checkOut();
                splitter.setContentHandler(tr.getInputHandler());
                splitter.parse(input);
                executor.execute(tr);
                n++;
            } while (splitter.hasMoreOutput(input));
            tobnew.notifyFinished();
            try {
                outputThread.join();
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            }
            executor.shutdown();
            docLevelEventBuffer.play(getContentHandler());
            System.out.println("total n="+n);
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
