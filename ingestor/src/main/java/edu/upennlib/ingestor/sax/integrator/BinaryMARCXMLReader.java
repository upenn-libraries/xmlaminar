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

package edu.upennlib.ingestor.sax.integrator;

import edu.upennlib.ingestor.sax.xsl.BufferingXMLFilter;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.MalformedInputException;
import java.sql.SQLException;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.sax.SAXTransformerFactory;
import javax.xml.transform.stream.StreamResult;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.TTCCLayout;
import org.marc4j.converter.impl.AnselToUnicode;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;

/**
 *
 * @author michael
 */
public class BinaryMARCXMLReader extends SQLXMLReader {

    public static final char RT = '\u001D';
    public static final char FT = '\u001E';
    public static final char DE = '\u001F';

    public BinaryMARCXMLReader() {
        super(InputStream.class);
    }

    private ByteArrayOutputStream baos = new ByteArrayOutputStream();
    private BufferingXMLFilter outputBuffer = new BufferingXMLFilter();
    private long currentId = -1;
    private boolean logRecordAsError = false;
    private String logRecordErrorMessage;
    private boolean printStackTraces = false;

    @Override
    protected void outputFieldAsSAXEvents(long selfId, String fieldLabel, Object rawContent) throws SAXException, IOException {
        if (selfId != currentId) {
            logRecordAsError = false;
            if (baos.size() > 0) {
                logger.error("discarding some output for record: "+currentId);
                baos.reset();
            }
            currentId = selfId;
        }
        if (rawContent != null) {
            InputStream content = (InputStream) rawContent;
            BufferedInputStream bis = new BufferedInputStream(content);
            int next = -1;
            boolean recordFinished = false;
            while ((next = bis.read()) != -1) {
                if (recordFinished) {
                    if (!logRecordAsError) {
                        logRecordAsError = true;
                        logRecordErrorMessage = "early record terminator on record " + currentId;
                    }
                } else if (next == RT) {
                    recordFinished = true;
                }
                baos.write(next);
            }
            if (recordFinished) {
                try {
                    parseRecord(baos.toByteArray());
                    outputBuffer.flush(ch);
                } catch (Exception e) {
                    if (!logRecordAsError) {
                        logRecordAsError = true;
                        logRecordErrorMessage = e.toString() + " on record "+currentId;
                    }
                    outputBuffer.clear();
                    if (printStackTraces) {
                        e.printStackTrace(System.out);
                    }
                }
                if (logRecordAsError) {
                    logger.error(logRecordErrorMessage);
                    FileOutputStream fos = new FileOutputStream("outputFiles/marcError/"+currentId+".mrc");
                    baos.writeTo(fos);
                    fos.close();
                }
                baos.reset();
            }
        }
    }
    private static final String lowBib = "3000620";
    private static final String highBib = "3000700";
    private static String host = "[host_or_ip]";
    private static String sid = "[sid]";
    private static String user = "[username]";
    private static String pwd = "[password]";
    private static String sql = "SELECT DISTINCT BIB_DATA.BIB_ID, BIB_DATA.SEQNUM, BIB_DATA.RECORD_SEGMENT "
            + "FROM PENNDB.BIB_DATA, BIB_MASTER "
            + "WHERE BIB_DATA.BIB_ID = BIB_MASTER.BIB_ID AND  BIB_MASTER.SUPPRESS_IN_OPAC = 'N' "
            + "AND BIB_DATA.BIB_ID > "+lowBib+" AND BIB_DATA.BIB_ID < "+highBib+" "
            + "ORDER BY BIB_ID, SEQNUM";

    public static void main(String args[]) throws ConnectionException, SQLException, FileNotFoundException, IOException, SAXException, TransformerConfigurationException, TransformerException, ParserConfigurationException {

        BinaryMARCXMLReader instance = new BinaryMARCXMLReader();
        instance.logger.addAppender(new ConsoleAppender(new TTCCLayout(), "System.out"));
        instance.logger.setLevel(Level.WARN);
        instance.setHost(host);
        instance.setSid(sid);
        instance.setUser(user);
        instance.setPwd(pwd);
        instance.setSql(sql);
        String[] ifl = {"BIB_ID"};
        instance.setIdFieldLabels(ifl);
        String[] ofl = {"RECORD_SEGMENT"};
        instance.setOutputFieldLabels(ofl);

        SAXTransformerFactory stf = (SAXTransformerFactory)TransformerFactory.newInstance(TRANSFORMER_FACTORY_CLASS_NAME, null);
        Transformer t = stf.newTransformer();
        t.setOutputProperty(OutputKeys.INDENT, "yes");
        BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream("/tmp/marc.xml"));
        long start = System.currentTimeMillis();
        t.transform(new SAXSource(instance, new InputSource()), new StreamResult(bos));
        System.out.println("duration: "+(System.currentTimeMillis() - start));
    }

    CharsetDecoder decodeUTF8 = Charset.forName("UTF8").newDecoder();
    CharsetDecoder decodeASCII = Charset.forName("ASCII").newDecoder();
    AnselToUnicode decodeMARC8 = new AnselToUnicode();
    boolean marc8 = false;

    int baseAddress;

    public void parseRecord(byte[] record) throws IOException, SAXException {
        CharBuffer leader = decodeASCII.decode(ByteBuffer.wrap(record, 0, 24));
        switch (leader.charAt(9)) {
            case 'a':
                marc8 = false;
                break;
            case ' ':
                marc8 = true;
                System.out.println("using marc8 charset on record: "+currentId);
                break;
            default:
                throw new IllegalStateException("unrecognized character coding declaration: '"+leader.charAt(9)+"'");
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
            try {
                field = decodeUTF8.decode(binaryField);
            } catch (MalformedInputException ex) {
                throw new IllegalStateException(ex.toString()+", tag="+tag+", binaryFieldLength="+(binaryField.limit() - binaryField.position()));
            }
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
        outputBuffer.startElement(MARCXML_URI, fieldType.localName, fieldType.qName, attRunner);
        handleFieldContents(field, fieldType);
        outputBuffer.endElement(MARCXML_URI, fieldType.localName, fieldType.qName);
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
                case DE:
                    if (inSubfield) {
                        outputBuffer.characters(field.array(), field.arrayOffset() + cStart, index - cStart);
                        outputBuffer.endElement(MARCXML_URI, FieldType.subfield.localName, FieldType.subfield.qName);
                    }
                    attRunner.clear();
                    attRunner.addAttribute(MARCXML_URI, FieldType.code.localName, FieldType.code.qName, "CDATA", Character.toString(field.charAt(++index)));
                    inSubfield = true;
                    outputBuffer.startElement(MARCXML_URI, FieldType.subfield.localName, FieldType.subfield.qName, attRunner);
                    cStart = -1;
                    break;
                case FT:
                    outputBuffer.characters(field.array(), field.arrayOffset() + cStart, index - cStart);
                    if (inSubfield) {
                        outputBuffer.endElement(MARCXML_URI, FieldType.subfield.localName, FieldType.subfield.qName);
                    }
                    finishedParsing = true;
                    if (index != field.length() - 1) {
                        logger.error("possible extra field terminator, record="+currentId);
                    }
            }
        }
        if (!finishedParsing) {
            throw new IllegalStateException("missing field terminator?");
        }
    }

    private void initializeOutput() throws SAXException {
        outputBuffer.startPrefixMapping(MARCXML_PREFIX, MARCXML_URI);
        attRunner.clear();
        outputBuffer.startElement(MARCXML_URI, FieldType.record.localName, FieldType.record.qName, attRunner);
    }

    private void finalizeOutput() throws SAXException {
        outputBuffer.endElement(MARCXML_URI, FieldType.record.localName, FieldType.record.qName);
        outputBuffer.endPrefixMapping(MARCXML_PREFIX);
    }

}
