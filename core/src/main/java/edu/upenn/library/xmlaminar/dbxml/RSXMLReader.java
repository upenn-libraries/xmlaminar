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

package edu.upenn.library.xmlaminar.dbxml;

import edu.upenn.library.xmlaminar.SAXFeatures;
import static edu.upenn.library.xmlaminar.XMLInputValidator.writeSanitizedXMLCharacters;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.sax.SAXTransformerFactory;
import javax.xml.transform.stream.StreamResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;

/**
 *
 * @author michael
 */
public class RSXMLReader extends SQLXMLReader {

    private final char[] characters = new char[2048];
    private final AttributesImpl attRunner = new AttributesImpl();
    private static final Logger logger = LoggerFactory.getLogger(RSXMLReader.class);
    private static final Map<String, Boolean> unmodifiableFeatures = new HashMap<String, Boolean>();

    static {
        unmodifiableFeatures.put(SAXFeatures.NAMESPACES, true);
        unmodifiableFeatures.put(SAXFeatures.NAMESPACE_PREFIXES, false);
        unmodifiableFeatures.put(SAXFeatures.STRING_INTERNING, true);
        unmodifiableFeatures.put(SAXFeatures.VALIDATION, false);
    }

    public RSXMLReader() {
        super(InputImplementation.CHAR_ARRAY, unmodifiableFeatures);
    }

    public RSXMLReader(int batchSize, int lookaheadFactor) {
        super(InputImplementation.CHAR_ARRAY, unmodifiableFeatures, batchSize, lookaheadFactor);
    }

    @Override
    protected void outputFieldAsSAXEvents(long selfId, String fieldLabel, char[] content, int endIndex) throws SAXException, IOException {
        if (content != null) {
            ch.startElement("", fieldLabel, fieldLabel, attRunner);
            writeSanitizedXMLCharacters(content, 0, endIndex, ch);
            ch.endElement("", fieldLabel, fieldLabel);
        }
    }

    @Override
    protected void outputFieldAsSAXEvents(long selfId, String fieldLabel, Reader content) throws SAXException, IOException {
        if (content != null) {
            ch.startElement("", fieldLabel, fieldLabel, attRunner);
            outputCharacters(content);
            ch.endElement("", fieldLabel, fieldLabel);
        }
    }

    @Override
    protected void outputFieldAsSAXEvents(long selfId, String fieldLabel, byte[] content) throws SAXException, IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void outputFieldAsSAXEvents(long selfId, String fieldLabel, InputStream content) throws SAXException, IOException {
        throw new UnsupportedOperationException();
    }

    private void outputCharacters(Reader content) throws IOException, SAXException {
        int length = -1;
        while ((length = content.read(characters, 0, characters.length)) != -1) {
            writeSanitizedXMLCharacters(characters, 0, length, ch);
        }
    }

    private static final String lowBib = "3032000";
    private static final String highBib = "3033000";
    private static String host = "[host_or_ip]";
    private static String sid = "[sid]";
    private static String user = "[username]";
    private static String pwd = "[password]";
    private static String sqlItem = "SELECT BM.BIB_ID, MI.MFHD_ID, I.ITEM_ID, MI.ITEM_ENUM, I.CREATE_DATE AS ITEM_CREATE_DATE, I.HOLDS_PLACED AS NUM_HOLDS, "
            + "L.LOCATION_NAME AS TEMP_LOCATION_DISP, L.LOCATION_DISPLAY_NAME AS TEMP_LOCATION "
            + "FROM BIB_MFHD BM, MFHD_ITEM MI, ITEM I LEFT OUTER JOIN LOCATION L ON I.TEMP_LOCATION = L.LOCATION_ID "
            + "WHERE BM.MFHD_ID = MI.MFHD_ID AND MI.ITEM_ID = I.ITEM_ID "
            + "AND BIB_ID > "+lowBib+" AND BIB_ID < "+highBib+" "
            + "ORDER BY BIB_ID, MFHD_ID, ITEM_ID";
    private static String[] itemIdFields = {"BIB_ID", "MFHD_ID", "ITEM_ID"};
    private static String sqlMfhd = "SELECT BM.BIB_ID, MM.MFHD_ID, MM.DISPLAY_CALL_NO, MM.NORMALIZED_CALL_NO, MM.CREATE_DATE AS HOLD_CREATE_DATE, MAX(ACTION_DATE) AS LAST_HOLD_UPDATE, CALL_NO_TYPE, "
            + "L.LOCATION_NAME AS PERM_LOCATION_DISP, L.LOCATION_DISPLAY_NAME AS PERM_LOCATION "
            + "FROM MFHD_MASTER MM, BIB_MFHD BM, MFHD_HISTORY MH, LOCATION L "
            + "WHERE MM.MFHD_ID = BM.MFHD_ID AND MH.MFHD_ID = BM.MFHD_ID AND MM.SUPPRESS_IN_OPAC = 'N' AND MM.LOCATION_ID = L.LOCATION_ID "
            + "AND BIB_ID > "+lowBib+" AND BIB_ID < "+highBib+" "
            + "GROUP BY BM.BIB_ID, MM.MFHD_ID, MM.DISPLAY_CALL_NO, MM.NORMALIZED_CALL_NO, MM.CREATE_DATE, CALL_NO_TYPE, L.LOCATION_NAME, L.LOCATION_DISPLAY_NAME "
            + "ORDER BY BIB_ID, MFHD_ID";
    private static String[] mfhdIdFields = {"BIB_ID", "MFHD_ID"};
    private static String sqlItemStatus = "SELECT BM.BIB_ID, MI.MFHD_ID, I.ITEM_ID, IST.ITEM_STATUS_TYPE AS STATUS_ID, IST.ITEM_STATUS_DESC STATUS "
            + "FROM BIB_MFHD BM, MFHD_ITEM MI, ITEM I, ITEM_STATUS, ITEM_STATUS_TYPE IST "
            + "WHERE BM.MFHD_ID = MI.MFHD_ID AND MI.ITEM_ID = I.ITEM_ID AND I.ITEM_ID = ITEM_STATUS.ITEM_ID AND ITEM_STATUS.ITEM_STATUS = IST.ITEM_STATUS_TYPE "
            + "AND BIB_ID > "+lowBib+" AND BIB_ID < "+highBib+" "
            + "ORDER BY BIB_ID, MFHD_ID, ITEM_ID, STATUS_ID";
    private static String[] itemStatusIdFields = {"BIB_ID", "MFHD_ID", "ITEM_ID", "STATUS_ID"};

    public static void main(String[] args) throws TransformerConfigurationException, TransformerException, FileNotFoundException, IOException {

        RSXMLReader instance = new RSXMLReader();
        instance.setDataSource(newDataSource(new File("work/connection.properties")));
        instance.setSql(sqlItemStatus);
        instance.setIdFieldLabels(itemStatusIdFields);

        SAXTransformerFactory stf = (SAXTransformerFactory)TransformerFactory.newInstance(TRANSFORMER_FACTORY_CLASS_NAME, null);
        Transformer t = stf.newTransformer();
        t.setOutputProperty(OutputKeys.INDENT, "yes");
        FileOutputStream fos = new FileOutputStream("/tmp/item_status.xml");
        BufferedOutputStream bos = new BufferedOutputStream(fos);
        t.transform(new SAXSource(instance, new InputSource()), new StreamResult(bos));
        bos.close();
    }

}
