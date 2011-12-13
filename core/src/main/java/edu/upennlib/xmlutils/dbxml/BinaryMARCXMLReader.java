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

package edu.upennlib.xmlutils.dbxml;

import edu.upennlib.dbutils.ConnectionException;
import edu.upennlib.xmlutils.SAXFeatures;
import edu.upennlib.xmlutils.UnboundedContentHandlerBuffer;
import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.MalformedInputException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
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
import org.apache.log4j.Logger;
import org.apache.log4j.TTCCLayout;
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
        super(InputImplementation.BYTE_ARRAY, unmodifiableFeatures);
    }

    private final UnboundedContentHandlerBuffer outputBuffer = new UnboundedContentHandlerBuffer();
    private long currentId = -1;
    private boolean logRecordAsError = false;
    private String logRecordErrorMessage;
    private boolean printStackTraces = false;
    private static final Logger logger = Logger.getLogger(BinaryMARCXMLReader.class);

    private static final int BYTE_BUFFER_INIT_SIZE = 2048;
    private byte[] byteBuffer = new byte[BYTE_BUFFER_INIT_SIZE];
    private int bufferTail = 0;

    private static final Map<String, Boolean> unmodifiableFeatures = new HashMap<String, Boolean>();

    static {
        unmodifiableFeatures.put(SAXFeatures.NAMESPACES, true);
        unmodifiableFeatures.put(SAXFeatures.NAMESPACE_PREFIXES, false);
        unmodifiableFeatures.put(SAXFeatures.STRING_INTERNING, true);
        unmodifiableFeatures.put(SAXFeatures.VALIDATION, false);
    }

    private void bufferBytes(byte[] content) {
        while (byteBuffer.length < bufferTail + content.length) {
            byte[] old = byteBuffer;
            byteBuffer = new byte[old.length * 2];
            System.arraycopy(old, 0, byteBuffer, 0, bufferTail);
        }
        System.arraycopy(content, 0, byteBuffer, bufferTail, content.length);
        bufferTail += content.length;
    }

    @Override
    protected void outputFieldAsSAXEvents(long selfId, String fieldLabel, byte[] content) throws SAXException, IOException {
        if (selfId != currentId) {
            logRecordAsError = false;
            if (bufferTail > 0) {
                logger.error("discarding some output for record: "+currentId);
                bufferTail = 0;
            }
            currentId = selfId;
        }
        if (content != null) {
            bufferBytes(content);
            if (byteBuffer[bufferTail - 1] == RT) {
                try {
                    parseRecord(byteBuffer, bufferTail);
                    outputBuffer.flush(ch, null);
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
                    BufferedOutputStream fos = new BufferedOutputStream(new FileOutputStream("outputFiles/marcError/"+currentId+".mrc"));
                    fos.write(byteBuffer, 0, bufferTail);
                    fos.close();
                }
                bufferTail = 0;
            }
        }
    }

    private static final int chunkSize = 2048;

    private void bufferBytes(InputStream content) throws IOException {
        int bytesRead = 0;
        do {
            bufferTail += bytesRead;
            if (byteBuffer.length < bufferTail + chunkSize) {
                byte[] old = byteBuffer;
                byteBuffer = new byte[old.length * 2];
                System.arraycopy(old, 0, byteBuffer, 0, bufferTail);
            }
        } while ((bytesRead = content.read(byteBuffer, bufferTail, chunkSize)) != -1);
    }

    @Override
    protected void outputFieldAsSAXEvents(long selfId, String fieldLabel, InputStream content) throws SAXException, IOException {
        if (selfId != currentId) {
            logRecordAsError = false;
            if (bufferTail > 0) {
                logger.error("discarding some output for record: "+currentId);
                bufferTail = 0;
            }
            currentId = selfId;
        }
        if (content != null) {
            bufferBytes(content);
            if (byteBuffer[bufferTail - 1] == RT) {
                try {
                    parseRecord(byteBuffer, bufferTail);
                    outputBuffer.flush(ch, null);
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
                    fos.write(byteBuffer, 0, bufferTail);
                    fos.close();
                }
                bufferTail = 0;
            }
        }
    }

    @Override
    protected void outputFieldAsSAXEvents(long selfId, String fieldLabel, char[] content, int endIndex) throws SAXException, IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void outputFieldAsSAXEvents(long selfId, String fieldLabel, Reader content) throws SAXException, IOException {
        throw new UnsupportedOperationException();
    }



    private static final String lowBib = "3032000";
    private static final String highBib = "3033000";
    private static String host = "[host_or_ip]";
    private static String sid = "[sid]";
    private static String user = "[username]";
    private static String pwd = "[password]";
    private static String sql = "SELECT DISTINCT BIB_DATA.BIB_ID, BIB_DATA.SEQNUM, BIB_DATA.RECORD_SEGMENT "
            + "FROM PENNDB.BIB_DATA, BIB_MASTER "
            + "WHERE BIB_DATA.BIB_ID = BIB_MASTER.BIB_ID AND  BIB_MASTER.SUPPRESS_IN_OPAC = 'N' "
            + "AND BIB_DATA.BIB_ID > "+lowBib+" AND BIB_DATA.BIB_ID < "+highBib+" "
            + "ORDER BY BIB_ID, SEQNUM";

    public static void main(String args[]) throws SQLException, FileNotFoundException, IOException, SAXException, TransformerConfigurationException, TransformerException, ParserConfigurationException, ConnectionException {

        logger.addAppender(new ConsoleAppender(new TTCCLayout(), "System.out"));
        logger.setLevel(Level.WARN);
        BinaryMARCXMLReader instance = getTestInstance();

        runTestInstanceToFile(instance);
    }

    public static BinaryMARCXMLReader getTestInstance() throws ConnectionException {
        BinaryMARCXMLReader instance = new BinaryMARCXMLReader();
        instance.setHost(host);
        instance.setSid(sid);
        instance.setUser(user);
        instance.setPwd(pwd);
        instance.setSql(sql);
        String[] ifl = {"BIB_ID"};
        instance.setIdFieldLabels(ifl);
        String[] ofl = {"RECORD_SEGMENT"};
        instance.setOutputFieldLabels(ofl);
        return instance;
    }

    public static void runTestInstanceToFile(BinaryMARCXMLReader instance) throws TransformerConfigurationException, TransformerException, IOException {
        SAXTransformerFactory stf = (SAXTransformerFactory)TransformerFactory.newInstance(TRANSFORMER_FACTORY_CLASS_NAME, null);
        Transformer t = stf.newTransformer();
        t.setOutputProperty(OutputKeys.INDENT, "yes");
        BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream("/tmp/marc.xml"));
        long start = System.currentTimeMillis();
        t.transform(new SAXSource(instance, new InputSource()), new StreamResult(bos));
        bos.close();
        System.out.println("duration: "+(System.currentTimeMillis() - start));
    }

    CharsetDecoder decodeUTF8 = Charset.forName("UTF8").newDecoder();
    CharsetDecoder decodeASCII = Charset.forName("ASCII").newDecoder();
    //AnselToUnicode decodeMARC8 = new AnselToUnicode();
    boolean marc8 = false;

    int baseAddress;

    public void parseRecord(byte[] record, int length) throws IOException, SAXException {
        CharBuffer leader = decodeASCII.decode(ByteBuffer.wrap(record, 0, 24));
        switch (leader.charAt(9)) {
            case 'a':
                marc8 = false;
                break;
            case ' ':
                marc8 = true;
                throw new UnsupportedEncodingException("does not currently support marc8 character encoding");
                //break;
            default:
                throw new IllegalStateException("unrecognized character coding declaration: '"+leader.charAt(9)+"'");
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

    public static final String MARCXML_URI = "http://www.loc.gov/MARC21/slim";
    public static final String MARCXML_PREFIX = "marc";

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
        final String uri;
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

    private MARCFieldModifier fieldModifier;

    public MARCFieldModifier getFieldModifier() {
        return fieldModifier;
    }

    public void setFieldModifier(MARCFieldModifier modifier) {
        fieldModifier = modifier;
    }

    private char[] out = new char[10];
    private final int[] startAndEnd = new int[2];

    private void parseVariableField(String tag, ByteBuffer binaryField) throws CharacterCodingException, SAXException {
        CharBuffer field;
        if (marc8) {
//            byte[] newBecauseMarc4jNeedsIt = new byte[binaryField.limit() - binaryField.position()]; // annoying!
//            binaryField.get(newBecauseMarc4jNeedsIt);
//            field = CharBuffer.wrap(decodeMARC8.convert(newBecauseMarc4jNeedsIt).toCharArray());
            throw new UnsupportedOperationException("marc8 character handling not implemented");
        } else {
            try {
                field = decodeUTF8.decode(binaryField);
            } catch (MalformedInputException ex) {
                throw new IllegalStateException(ex.toString() + ", tag=" + tag + ", binaryFieldLength=" + (binaryField.limit() - binaryField.position()));
            }
        }
        char[] array;
        if (fieldModifier == null || (array = fieldModifier.modifyField(tag, field, out, startAndEnd)) == null) {
            array = field.array();
            startAndEnd[0] = field.arrayOffset() + field.position();
            startAndEnd[1] = startAndEnd[0] + field.length();
        } else {
            out = array;
        }
        attRunner.clear();
        attRunner.addAttribute(FieldType.tag.uri, FieldType.tag.localName, FieldType.tag.qName, "CDATA", tag);
        FieldType fieldType;
        if (tag.startsWith("00")) {
            fieldType = FieldType.controlfield;
        } else {
            attRunner.addAttribute(FieldType.ind1.uri, FieldType.ind1.localName, FieldType.ind1.qName, "CDATA", Character.toString(field.charAt(0)));
            attRunner.addAttribute(FieldType.ind2.uri, FieldType.ind2.localName, FieldType.ind2.qName, "CDATA", Character.toString(field.charAt(1)));
            fieldType = FieldType.datafield;
        }
        outputBuffer.startElement(fieldType.uri, fieldType.localName, fieldType.qName, attRunner);
        handleFieldContents(array, startAndEnd[0], startAndEnd[1], fieldType);
        outputBuffer.endElement(fieldType.uri, fieldType.localName, fieldType.qName);
    }

    private void handleFieldContents(char[] field, int start, int end, FieldType fieldType) throws SAXException {
        boolean inSubfield = false;
        int cStart = -1;
        int index;
        switch (fieldType) {
            case controlfield:
                index = start - 1;
                break;
            default:
                index = start + 1;
        }
        boolean finishedParsing = false;
        while (!finishedParsing && ++index < end) {
            if (cStart < 0) {
                cStart = index;
            }
            switch (field[index]) {
                case DE:
                    if (inSubfield) {
                        outputBuffer.characters(field, cStart, index - cStart);
                        outputBuffer.endElement(FieldType.subfield.uri, FieldType.subfield.localName, FieldType.subfield.qName);
                    }
                    attRunner.clear();
                    attRunner.addAttribute(FieldType.code.uri, FieldType.code.localName, FieldType.code.qName, "CDATA", Character.toString(field[++index]));
                    inSubfield = true;
                    outputBuffer.startElement(FieldType.subfield.uri, FieldType.subfield.localName, FieldType.subfield.qName, attRunner);
                    cStart = -1;
                    break;
                case FT:
                    outputBuffer.characters(field, cStart, index - cStart);
                    if (inSubfield) {
                        outputBuffer.endElement(FieldType.subfield.uri, FieldType.subfield.localName, FieldType.subfield.qName);
                    }
                    finishedParsing = true;
                    if (index != end - 1) {
                        logger.error("possible extra field terminator, record="+currentId);
                    }
            }
        }
        if (!finishedParsing) {
            throw new IllegalStateException("missing field terminator?");
        }
    }

    private void initializeOutput(CharBuffer leader) throws SAXException {
        outputBuffer.startPrefixMapping(MARCXML_PREFIX, MARCXML_URI);
        attRunner.clear();
        outputBuffer.startElement(FieldType.record.uri, FieldType.record.localName, FieldType.record.qName, attRunner);
        outputBuffer.startElement(FieldType.leader.uri, FieldType.leader.localName, FieldType.leader.qName, attRunner);
        outputBuffer.characters(leader.array(), leader.arrayOffset(), 24);
        outputBuffer.endElement(FieldType.leader.uri, FieldType.leader.localName, FieldType.leader.qName);
    }

    private void finalizeOutput() throws SAXException {
        outputBuffer.endElement(FieldType.record.uri, FieldType.record.localName, FieldType.record.qName);
        outputBuffer.endPrefixMapping(MARCXML_PREFIX);
    }

}
