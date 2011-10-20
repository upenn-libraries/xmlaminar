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
 * Copyright 2010 Trustees of the University of Pennsylvania Licensed under the
 * Educational Community License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.osedu.org/licenses/ECL-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package edu.upennlib.ingestor.sax.integrator;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.MalformedInputException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.sax.SAXTransformerFactory;
import javax.xml.transform.sax.TransformerHandler;
import javax.xml.transform.stream.StreamResult;
import org.apache.log4j.Logger;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;

/**
 * A utility class to write binary MARC records to a SAX ContentHandler as MARCXML.
 * Not thread-safe.
 *
 * @author Michael Gibney
 */
public class MarcBinaryToSAX {

    public static void main(String[] args) throws FileNotFoundException, TransformerConfigurationException, IOException, SAXException {
        String sampleMarcBinaryFilePath = "/home/michael/Downloads/sample_record_files/sample_rec.mrc";
        FileInputStream fis = new FileInputStream(sampleMarcBinaryFilePath);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte[] buffer = new byte[2048];
        int bytesRead;
        while ((bytesRead = fis.read(buffer)) != -1) {
            baos.write(buffer, 0, bytesRead);
        }
        SAXTransformerFactory tf = (SAXTransformerFactory)TransformerFactory.newInstance("net.sf.saxon.TransformerFactoryImpl", null);
        TransformerHandler th = tf.newTransformerHandler();
        th.getTransformer().setOutputProperty(OutputKeys.INDENT, "yes");
        th.setResult(new StreamResult(System.out));
        MarcBinaryToSAX instance = new MarcBinaryToSAX();
        instance.setContentHandler(th);
        th.startDocument();
        instance.parseRecord(baos.toByteArray());
        th.endDocument();
    }

    public static final char RECORD_TERMINATOR = '\u001D';
    public static final char FIELD_TERMINATOR = '\u001E';
    public static final char DELIMITER = '\u001F';

    private final CharsetDecoder decodeUTF8 = Charset.forName("UTF8").newDecoder();
    private final CharsetDecoder decodeASCII = Charset.forName("ASCII").newDecoder();

    private boolean marc8 = false;

    private static final Logger logger = Logger.getLogger(MarcBinaryToSAX.class);

    private int baseAddress;

    public static final String MARCXML_URI = "http://www.loc.gov/MARC21/slim";
    public static final String MARCXML_PREFIX = "marc";

    private final AttributesImpl atts = new AttributesImpl();

    private ContentHandler output;

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
        final String uri;
        final String prefix;
        final String localName;
        final String qName;
        FieldType(String localName) {
            this.localName = localName;
            if (localName.equals("tag") || localName.equals("ind1") || localName.equals("ind2") || localName.equals("code")) {
                this.uri = "";
                this.prefix = "";
                this.qName = localName;
            } else {
                this.uri = MARCXML_URI;
                this.prefix = MARCXML_PREFIX;
                this.qName = MARCXML_PREFIX+":"+localName;
            }
        }
    }

    public ContentHandler getContentHandler() {
        return output;
    }

    public void setContentHandler(ContentHandler handler) {
        output = handler;
    }

    /**
     * Parse the given MARC record and write the contents as MARCXML to this
     * instance's ContentHandler.
     *
     * @param record
     * @throws IOException
     * @throws SAXException
     */
    public void parseRecord(byte[] record) throws IOException, SAXException {
        CharBuffer leader = decodeASCII.decode(ByteBuffer.wrap(record, 0, 24));
        switch (leader.charAt(9)) {
            case 'a':
                marc8 = false;
                break;
            case ' ':
                marc8 = true;
                throw new UnsupportedOperationException("parsing marc8-encoded records not currently supported");
                //break;
            default:
                throw new IllegalStateException("unrecognized character encoding declaration: '"+leader.charAt(9)+"'");
        }
        baseAddress = Integer.parseInt(leader.subSequence(12, 17).toString());
        CharBuffer directory = decodeASCII.decode(ByteBuffer.wrap(record, 24, baseAddress - 24));
        int directoryIndex = 0;
        initializeOutput(leader);
        while (directoryIndex < directory.length() - 1) {
            String tag = directory.subSequence(directoryIndex, directoryIndex + 3).toString();
            int fieldLength = Integer.parseInt(directory.subSequence(directoryIndex + 3, directoryIndex + 7).toString());
            int fieldStart = Integer.parseInt(directory.subSequence(directoryIndex + 7, directoryIndex + 12).toString());
            parseVariableField(tag, ByteBuffer.wrap(record, baseAddress+fieldStart, fieldLength));
            directoryIndex += 12;
        }
        finalizeOutput();
    }

    private void parseVariableField(String tag, ByteBuffer binaryField) throws CharacterCodingException, SAXException {
        CharBuffer field;
        if (marc8) {
            throw new UnsupportedOperationException("parsing marc8-encoded records not currently supported");
        } else {
            try {
                field = decodeUTF8.decode(binaryField);
            } catch (MalformedInputException ex) {
                throw new IllegalStateException(ex.toString()+", tag="+tag+", binaryFieldLength="+(binaryField.limit() - binaryField.position()));
            }
        }
        atts.clear();
        atts.addAttribute("", FieldType.tag.localName, FieldType.tag.qName, "CDATA", tag);
        FieldType fieldType;
        if (tag.startsWith("00")) {
            fieldType = FieldType.controlfield;
        } else {
            atts.addAttribute("", FieldType.ind1.localName, FieldType.ind1.qName, "CDATA", Character.toString(field.charAt(0)));
            atts.addAttribute("", FieldType.ind2.localName, FieldType.ind2.qName, "CDATA", Character.toString(field.charAt(1)));
            fieldType = FieldType.datafield;
        }
        output.startElement(fieldType.uri, fieldType.localName, fieldType.qName, atts);
        handleFieldContents(field, fieldType);
        output.endElement(fieldType.uri, fieldType.localName, fieldType.qName);
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
        while (!finishedParsing && ++index < field.length()) {
            if (cStart < 0) {
                cStart = index;
            }
            switch (field.charAt(index)) {
                case DELIMITER:
                    if (inSubfield) {
                        output.characters(field.array(), field.arrayOffset() + cStart, index - cStart);
                        output.endElement(FieldType.subfield.uri, FieldType.subfield.localName, FieldType.subfield.qName);
                    }
                    atts.clear();
                    atts.addAttribute(FieldType.code.uri, FieldType.code.localName, FieldType.code.qName, "CDATA", Character.toString(field.charAt(++index)));
                    inSubfield = true;
                    output.startElement(FieldType.subfield.uri, FieldType.subfield.localName, FieldType.subfield.qName, atts);
                    cStart = -1;
                    break;
                case FIELD_TERMINATOR:
                    output.characters(field.array(), field.arrayOffset() + cStart, index - cStart);
                    if (inSubfield) {
                        output.endElement(FieldType.subfield.uri, FieldType.subfield.localName, FieldType.subfield.qName);
                    }
                    finishedParsing = true;
                    if (index != field.length() - 1) {
                        logger.error("possible extra field terminator");
                    }
            }
        }
        if (!finishedParsing) {
            throw new IllegalStateException("missing field terminator?");
        }
    }

    private void initializeOutput(CharBuffer leader) throws SAXException {
        output.startPrefixMapping(MARCXML_PREFIX, MARCXML_URI);
        atts.clear();
        output.startElement(FieldType.record.uri, FieldType.record.localName, FieldType.record.qName, atts);
        output.startElement(FieldType.leader.uri, FieldType.leader.localName, FieldType.leader.qName, atts);
        output.characters(leader.array(), leader.arrayOffset(), 24);
        output.endElement(FieldType.leader.uri, FieldType.leader.localName, FieldType.leader.qName);
    }

    private void finalizeOutput() throws SAXException {
        output.endElement(FieldType.record.uri, FieldType.record.localName, FieldType.record.qName);
        output.endPrefixMapping(MARCXML_PREFIX);
    }

}
