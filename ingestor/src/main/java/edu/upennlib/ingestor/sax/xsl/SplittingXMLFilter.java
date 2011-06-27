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
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.stream.StreamResult;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.AttributesImpl;
import org.xml.sax.helpers.XMLFilterImpl;

/**
 *
 * @author michael
 */
public class SplittingXMLFilter extends MyXFI {

    public final boolean ENFORCE_RECORD_LEVEL_ELEMENT_CONSISTENCY = true;
    private final boolean[] parsing = new boolean[1];
    boolean parsingInitiated = false;
    public final int DEFAULT_CHUNK_SIZE = 20;
    private int chunkSize = DEFAULT_CHUNK_SIZE;
    public final boolean BUFFERING = false; // Doesn't actually help.
    public final int RECORD_LEVEL = 1;
    private String[] recordElementIdentifiers = null;
    private int workingRecordCount = 0;
    private int level = -1;
    private Boolean inRecord = null;
    private boolean encounteredFirstRecord = false;
    private MyEndEventStack endEventStack = new MyEndEventStack();
    private BufferingXMLFilter startEvents = new BufferingXMLFilter();
    private SaxEventExecutor executor = new SaxEventExecutor();

    public static void main(String[] args) throws FileNotFoundException, SAXException, IOException, ParserConfigurationException, TransformerConfigurationException, TransformerException {
        SplittingXMLFilter instance = new SplittingXMLFilter();
        SAXParserFactory spf = SAXParserFactory.newInstance();
        spf.setNamespaceAware(true);
        SAXParser parser = spf.newSAXParser();
        instance.setParent(parser.getXMLReader());
        BufferedInputStream bis = new BufferedInputStream(new FileInputStream("/tmp/large.xml"));
        InputSource inputSource = new InputSource(bis);
        Transformer t = TransformerFactory.newInstance().newTransformer();
        long start = System.currentTimeMillis();
        int nextFileNumber = 0;
        do {
            File file = new File("/tmp/splitOutput/testSplitOut" + String.format("%05d", nextFileNumber++) + ".xml");
            BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(file));
            t.transform(new SAXSource(instance, inputSource), new StreamResult(bos));
        } while (instance.hasMoreOutput(inputSource));
        System.out.println("duration: "+(System.currentTimeMillis() - start));
    }

    public void setChunkSize(int chunkSize) {
        this.chunkSize = chunkSize;
    }

    private boolean checkRotate(String uri, String localName, String name, Attributes atts) {
        if (level != RECORD_LEVEL) {
            return false;
        } else {
            encounteredFirstRecord = true;
            endEventStack.lock();
            if (ENFORCE_RECORD_LEVEL_ELEMENT_CONSISTENCY) {
                if (recordElementIdentifiers == null) {
                    if (level == RECORD_LEVEL) {
                        recordElementIdentifiers = new String[3];
                        recordElementIdentifiers[0] = uri;
                        recordElementIdentifiers[1] = localName;
                        recordElementIdentifiers[2] = name;
                    }
                }
                if (recordElementIdentifiers != null && recordElementIdentifiers[0].equals(uri) && recordElementIdentifiers[1].equals(localName) && recordElementIdentifiers[2].equals(name)) {
                    inRecord = true;
                } else {
                    throw new IllegalStateException("record level element consistency check failed"+recordElementIdentifiers[2]+"?="+name);
                }
            }
            return workingRecordCount >= chunkSize;
        }
    }

    private void writeSyntheticEndEvents() {
        try {
            level += endEventStack.writeEndEvents(getContentHandler());
        } catch (SAXException ex) {
            throw new RuntimeException(ex);
        }
            synchronized (parsing) {
                parsing[0] = false;
                parsing.notify();
                try {
                    parsing.wait();
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
            }
    }

    private void writeSyntheticStartEvents() {
        try {
            level += startEvents.play(getContentHandler());
        } catch (SAXException ex) {
            throw new RuntimeException(ex);
        }
        try {
            if (tmpStartElement != null) {
                level += executor.executeSaxEvent(this, tmpStartElement, true, true);
            }
        } catch (SAXException ex) {
            throw new RuntimeException(ex);
        }
        tmpStartElement = null;
    }

    public boolean hasMoreOutput(InputSource input) {
        if (!inputs.containsKey(input)) {
            return false;
        } else {
            return inputs.get(input).hasMoreOutputL;
        }
    }

    private Object[] tmpStartElement = null;
    private void storeTmpStartElement(Object... args) {
        tmpStartElement = args;
    }

    @Override
    public void startElement(String uri, String localName, String name, Attributes atts) throws SAXException {
        //System.out.println(Thread.currentThread().getName()+" incrementing level");
        level++;
        if (checkRotate(uri, localName, name, atts)) {
            storeTmpStartElement(SaxEventType.startElement, uri, localName, name, atts);
            writeSyntheticEndEvents();
        } else {
            Attributes myAtt = new AttributesImpl(atts);
            if (!encounteredFirstRecord) {
                startEvents.startElement(uri, localName, name, myAtt);
                endEventStack.startElement(uri, localName, name, myAtt);
            }
            super.startElement(uri, localName, name, atts);
        }
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        if (!encounteredFirstRecord) {
            startEvents.endElement(uri, localName, qName);
            endEventStack.endElement(uri, localName, qName);
        }
        super.endElement(uri, localName, qName);
        //System.out.println(Thread.currentThread().getName()+" decrementing level");
        if (level-- == RECORD_LEVEL) {
            if (ENFORCE_RECORD_LEVEL_ELEMENT_CONSISTENCY) {
                if (!inRecord) {
                    throw new IllegalStateException();
                }
                inRecord = false;
            }
            workingRecordCount++;
        }
    }

    @Override
    public void endDocument() throws SAXException {
        if (!encounteredFirstRecord) {
            startEvents.endDocument();
            endEventStack.endDocument();
        }
        super.endDocument();
        currentInputState.hasMoreOutputL = false;
        synchronized (parsing) {
            parsing[0] = false;
            parsing.notify();
        }
    }

    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        if (!encounteredFirstRecord) {
            startEvents.characters(ch, start, length);
        }
        super.characters(ch, start, length);
    }

    @Override
    public void endPrefixMapping(String prefix) throws SAXException {
        if (!encounteredFirstRecord) {
            startEvents.endPrefixMapping(prefix);
            endEventStack.endPrefixMapping(prefix);
        }
        super.endPrefixMapping(prefix);
    }

    @Override
    public void error(SAXParseException e) throws SAXException {
        if (!encounteredFirstRecord) {
            startEvents.error(e);
        }
        super.error(e);
    }

    @Override
    public void fatalError(SAXParseException e) throws SAXException {
        if (!encounteredFirstRecord) {
            startEvents.fatalError(e);
        }
        super.fatalError(e);
    }

    @Override
    public void ignorableWhitespace(char[] ch, int start, int length) throws SAXException {
        if (!encounteredFirstRecord) {
            startEvents.ignorableWhitespace(ch, start, length);
        }
        super.ignorableWhitespace(ch, start, length);
    }

    @Override
    public void notationDecl(String name, String publicId, String systemId) throws SAXException {
        if (!encounteredFirstRecord) {
            startEvents.notationDecl(name, publicId, systemId);
        }
        super.notationDecl(name, publicId, systemId);
    }

    @Override
    public void processingInstruction(String target, String data) throws SAXException {
        if (!encounteredFirstRecord) {
            startEvents.processingInstruction(target, data);
        }
        super.processingInstruction(target, data);
    }

    @Override
    public InputSource resolveEntity(String publicId, String systemId) throws SAXException, IOException {
        if (!encounteredFirstRecord) {
            startEvents.resolveEntity(publicId, systemId);
        }
        return super.resolveEntity(publicId, systemId);
    }

    @Override
    public void setDocumentLocator(Locator locator) {
        if (!encounteredFirstRecord) {
            startEvents.setDocumentLocator(locator);
        }
        super.setDocumentLocator(locator);
    }

    @Override
    public void skippedEntity(String name) throws SAXException {
        if (!encounteredFirstRecord) {
            startEvents.skippedEntity(name);
        }
        super.skippedEntity(name);
    }

    @Override
    public void startDocument() throws SAXException {
        if (!encounteredFirstRecord) {
            startEvents.startDocument();
            endEventStack.startDocument();
        }
        super.startDocument();
    }

    @Override
    public void startPrefixMapping(String prefix, String uri) throws SAXException {
        if (!encounteredFirstRecord) {
            startEvents.startPrefixMapping(prefix, uri);
            endEventStack.startPrefixMapping(prefix, uri);
        }
        super.startPrefixMapping(prefix, uri);
    }

    @Override
    public void unparsedEntityDecl(String name, String publicId, String systemId, String notationName) throws SAXException {
        if (!encounteredFirstRecord) {
            startEvents.unparsedEntityDecl(name, publicId, systemId, notationName);
        }
        super.unparsedEntityDecl(name, publicId, systemId, notationName);
    }

    @Override
    public void warning(SAXParseException e) throws SAXException {
        if (!encounteredFirstRecord) {
            startEvents.warning(e);
        }
        super.warning(e);
    }

    @Override
    public void comment(char[] ch, int start, int length) throws SAXException {
        if (!encounteredFirstRecord) {
            startEvents.comment(ch, start, length);
        }
        super.comment(ch, start, length);
    }

    @Override
    public void endCDATA() throws SAXException {
        if (!encounteredFirstRecord) {
            startEvents.endCDATA();
            endEventStack.endCDATA();
        }
        super.endCDATA();
    }

    @Override
    public void endDTD() throws SAXException {
        if (!encounteredFirstRecord) {
            startEvents.endDTD();
            endEventStack.endDTD();
        }
        super.endDTD();
    }

    @Override
    public void endEntity(String name) throws SAXException {
        if (!encounteredFirstRecord) {
            startEvents.endEntity(name);
            endEventStack.endEntity(name);
        }
        super.endEntity(name);
    }

    @Override
    public void startCDATA() throws SAXException {
        if (!encounteredFirstRecord) {
            startEvents.startCDATA();
            endEventStack.startCDATA();
        }
        super.startCDATA();
    }

    @Override
    public void startDTD(String name, String publicId, String systemId) throws SAXException {
        if (!encounteredFirstRecord) {
            startEvents.startDTD(name, publicId, systemId);
            endEventStack.startDTD(name, publicId, systemId);
        }
        super.startDTD(name, publicId, systemId);
    }

    @Override
    public void startEntity(String name) throws SAXException {
        if (!encounteredFirstRecord) {
            startEvents.startEntity(name);
            endEventStack.startEntity(name);
        }
        super.startEntity(name);
    }



    InputState currentInputState = null;
    @Override
    public void parse(InputSource input) throws SAXException, IOException {
        synchronized(parsing) {
            while (parsing[0]) {
                try {
                    parsing.wait();
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
                if (!hasMoreOutput(input)) {
                    return;
                }
            }
        }
        workingRecordCount = 0;
        level = -1;
        InputState inputState;
        if (inputs.containsKey(input)) {
            inputState = inputs.get(input);
        } else {
            inputState = new InputState();
            inputState.input = input;
            inputs.put(input, inputState);
        }
        currentInputState = inputState;
        if (!inputState.hasMoreOutputL) {
            inputState.hasMoreOutputL = true;
            if (BUFFERING) {
                inputState.inputBuffer.setParent(getParent());
                setParent(inputState.inputBuffer);
            }
            XMLReader parent = getParent();
            parent.setContentHandler(this);
            parent.setProperty("http://xml.org/sax/properties/lexical-handler", this);
            parent.setDTDHandler(this);
            parent.setEntityResolver(this);
            parent.setErrorHandler(this);
            SuperParser sp = new SuperParser(parent, input);
            inputState.superParserThreadL = new Thread(sp, "superParser");
            inputState.superParserThreadL.start();
        } else {
            writeSyntheticStartEvents();
        }
        synchronized (parsing) {
            parsing[0] = true;
            parsing.notify();
            try {
                parsing.wait();
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    private class SuperParser implements Runnable {

        private XMLReader superReader;
        private InputSource inputSource;
        private String systemId;

        public SuperParser(XMLReader superReader, InputSource inputSource) {
            this.superReader = superReader;
            this.inputSource = inputSource;
        }

        public SuperParser(XMLReader superReader, String systemId) {
            this.superReader = superReader;
            this.systemId = systemId;
        }

        @Override
        public void run() {
            try {
                if (inputSource != null && systemId == null) {
                    superReader.parse(inputSource);
                } else if (systemId != null && inputSource == null) {
                    superReader.parse(systemId);
                }
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            } catch (SAXException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    @Override
    public void parse(String systemId) throws SAXException, IOException {
        throw new UnsupportedOperationException();
    }

    private HashMap<Object, InputState> inputs = new HashMap<Object, InputState>();

    private static class InputState {
        InputSource input;
        boolean hasMoreOutputL = false;
        BufferingXMLFilter inputBuffer = new BufferingXMLFilter();
        Thread superParserThreadL;
    }


}
