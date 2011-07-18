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

package edu.upennlib.ingestor.sax.testutils;

import edu.upennlib.ingestor.sax.utils.ConnectionException;
import edu.upennlib.ingestor.sax.utils.NoopXMLFilter;
import edu.upennlib.ingestor.sax.xsl.BufferingXMLFilter;
import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.sql.SQLException;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.stream.StreamResult;
import org.marc4j.converter.impl.AnselToUnicode;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;

/**
 *
 * @author michael
 */
public class DBMarcToFile {

    public static final char RT = '\u001D';
    public static final char FT = '\u001E';
    public static final char DE = '\u001F';

    public static final String TRANSFORMER_FACTORY_CLASS_NAME = "net.sf.saxon.TransformerFactoryImpl";
    public static void main(String args[]) throws ConnectionException, SQLException, FileNotFoundException, IOException, SAXException, TransformerConfigurationException, TransformerException, ParserConfigurationException {

        FileInputStream fis = new FileInputStream("inputFiles/marc/1.mrc");
        DBMarcToFile instance = new DBMarcToFile();
        BufferingXMLFilter bxf = new BufferingXMLFilter();
        instance.ch = bxf;
        instance.parseRecord(fis);
        fis.close();
        TransformerFactory tf = TransformerFactory.newInstance(TRANSFORMER_FACTORY_CLASS_NAME, null);
        Transformer t = tf.newTransformer();
        SAXParserFactory spf = SAXParserFactory.newInstance();
        spf.setNamespaceAware(true);
        NoopXMLFilter noop = new NoopXMLFilter();
        noop.setParent(spf.newSAXParser().getXMLReader());
        bxf.setParent(noop);
        t.setOutputProperty(OutputKeys.INDENT, "yes");
        t.transform(new SAXSource(bxf, new InputSource()), new StreamResult(System.out));

    }

    CharsetDecoder decodeUTF8 = Charset.forName("UTF8").newDecoder();
    CharsetDecoder decodeASCII = Charset.forName("ASCII").newDecoder();
    AnselToUnicode decodeMARC8 = new AnselToUnicode();
    boolean marc8 = false;

    int baseAddress;

    public void parseRecord(InputStream marcIn) throws IOException, SAXException {
        BufferedInputStream bis = new BufferedInputStream(marcIn);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        int next = -1;
        while ((next = bis.read()) != -1) {
            baos.write(next);
        }
        byte[] record = baos.toByteArray();
        CharBuffer leader = decodeASCII.decode(ByteBuffer.wrap(record, 0, 24));
        switch (leader.charAt(9)) {
            case 'a':
                marc8 = false;
                break;
            case ' ':
                marc8 = true;
                break;
            default:
                throw new RuntimeException("unrecognized character coding declaration: '"+leader.charAt(9)+"'");
        }
        baseAddress = Integer.parseInt(leader.subSequence(12, 17).toString());
        CharBuffer directory = decodeASCII.decode(ByteBuffer.wrap(record, 24, baseAddress - 24));
        int directoryIndex = 0;
        initializeOutput();
        while (directoryIndex < directory.length() - 1) {
            String tag = directory.subSequence(directoryIndex, directoryIndex + 3).toString();
            int fieldLength = Integer.parseInt(directory.subSequence(directoryIndex + 3, directoryIndex + 7).toString());
            int fieldStart = Integer.parseInt(directory.subSequence(directoryIndex + 7, directoryIndex + 12).toString());
            parseVariableField(tag, ByteBuffer.wrap(record, baseAddress+fieldStart, fieldLength));
            directoryIndex += 12;
        }
        finalizeOutput();
    }

    static final String MARCXML_URI = "http://www.loc.gov/MARC21/slim";
    static final String MARCXML_PREFIX = "marc";

    private AttributesImpl attRunner = new AttributesImpl();

    private static enum FieldType {
        record("record"),
        leader("leader"),
        controlfield("controlfield"),
        datafield("datafield"),
        tag("tag"),
        ind1("ind1"),
        ind2("ind2"),
        subfield("subfield"),
        code("code");
        final String prefix;
        final String localName;
        final String qName;
        FieldType(String localName) {
            this.localName = localName;
            if (localName.equals("tag") || localName.equals("ind1") || localName.equals("ind2") || localName.equals("code")) {
                this.prefix = "";
                this.qName = localName;
            } else {
                this.prefix = MARCXML_PREFIX;
                this.qName = MARCXML_PREFIX+":"+localName;
            }
        }
    }

    private void parseVariableField(String tag, ByteBuffer binaryField) throws CharacterCodingException, SAXException {
        CharBuffer field;
        if (marc8) {
            byte[] newBecauseMarc4jNeedsIt = new byte[binaryField.limit() - binaryField.position()]; // annoying!
            binaryField.get(newBecauseMarc4jNeedsIt);
            field = CharBuffer.wrap(decodeMARC8.convert(newBecauseMarc4jNeedsIt).toCharArray());
        } else {
            field = decodeUTF8.decode(binaryField);
        }
        attRunner.clear();
        attRunner.addAttribute(MARCXML_URI, FieldType.tag.localName, FieldType.tag.qName, "CDATA", tag);
        FieldType fieldType;
        if (tag.startsWith("00")) {
            fieldType = FieldType.controlfield;
        } else {
            attRunner.addAttribute(MARCXML_URI, FieldType.ind1.localName, FieldType.ind1.qName, "CDATA", Character.toString(field.charAt(0)));
            attRunner.addAttribute(MARCXML_URI, FieldType.ind2.localName, FieldType.ind2.qName, "CDATA", Character.toString(field.charAt(1)));
            fieldType = FieldType.datafield;
        }
        ch.startElement(MARCXML_URI, fieldType.localName, fieldType.qName, attRunner);
        handleFieldContents(field, fieldType);
        ch.endElement(MARCXML_URI, fieldType.localName, fieldType.qName);
    }

    private void handleFieldContents(CharBuffer field, FieldType fieldType) throws SAXException {
        boolean inSubfield = false;
        int cStart = -1;
        int index;
        switch (fieldType) {
            case controlfield:
                index = -1;
                break;
            default:
                index = 1;
        }
        boolean finishedParsing = false;
        while (++index < field.length()) {
            if (cStart < 0) {
                cStart = index;
            }
            switch (field.charAt(index)) {
                case DE:
                    if (inSubfield) {
                        ch.characters(field.array(), field.arrayOffset() + cStart, index - cStart - 1);
                        ch.endElement(MARCXML_URI, FieldType.subfield.localName, FieldType.subfield.qName);
                    }
                    attRunner.clear();
                    attRunner.addAttribute(MARCXML_URI, FieldType.code.localName, FieldType.code.qName, "CDATA", Character.toString(field.charAt(++index)));
                    inSubfield = true;
                    ch.startElement(MARCXML_URI, FieldType.subfield.localName, FieldType.subfield.qName, attRunner);
                    cStart = -1;
                    break;
                case FT:
                    ch.characters(field.array(), field.arrayOffset() + cStart, index - cStart);
                    if (inSubfield) {
                        ch.endElement(MARCXML_URI, FieldType.subfield.localName, FieldType.subfield.qName);
                    }
                    if (index == field.length() - 1) {
                        finishedParsing = true;
                    }
            }
        }
        if (!finishedParsing) {
            throw new IllegalStateException();
        }
    }

    ContentHandler ch;

    private void initializeOutput() throws SAXException {
        ch.startDocument();
        ch.startPrefixMapping(MARCXML_PREFIX, MARCXML_URI);
        attRunner.clear();
        ch.startElement(MARCXML_URI, FieldType.record.localName, FieldType.record.qName, attRunner);
    }

    private void finalizeOutput() throws SAXException {
        ch.endElement(MARCXML_URI, FieldType.record.localName, FieldType.record.qName);
        ch.endPrefixMapping(MARCXML_PREFIX);
        ch.endDocument();
    }

}
