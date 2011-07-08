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

package edu.upennlib.ingestor.sax.integrator.complex;

import edu.upennlib.ingestor.sax.integrator.BinaryMARCXMLReader;
import edu.upennlib.ingestor.sax.integrator.ConnectionException;
import edu.upennlib.ingestor.sax.integrator.RSXMLReader;
import edu.upennlib.ingestor.sax.xsl.BufferingXMLFilter;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.OutputKeys;
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
public class IntegratorSAX {
    private static final String lowBib = "3000000";
    private static final String highBib = "3100000";
    private static String host = "[host_or_ip]";
    private static String sid = "[sid]";
    private static String user = "[username]";
    private static String pwd = "[password]";
    private static String marcSql = "SELECT DISTINCT BIB_DATA.BIB_ID, BIB_DATA.SEQNUM, BIB_DATA.RECORD_SEGMENT "
            + "FROM PENNDB.BIB_DATA, BIB_MASTER "
            + "WHERE BIB_DATA.BIB_ID = BIB_MASTER.BIB_ID AND  BIB_MASTER.SUPPRESS_IN_OPAC = 'N' "
            + "AND BIB_DATA.BIB_ID > "+lowBib+" AND BIB_DATA.BIB_ID < "+highBib+" "
            + "ORDER BY BIB_ID, SEQNUM";
    private static String[] marcIdFields = {"BIB_ID"};
    private static String[] marcOutputFields = {"RECORD_SEGMENT"};
    private static String itemSql = "SELECT BM.BIB_ID, MI.MFHD_ID, I.ITEM_ID, MI.ITEM_ENUM, I.CREATE_DATE AS ITEM_CREATE_DATE, I.HOLDS_PLACED AS NUM_HOLDS, "
            + "L.LOCATION_NAME AS TEMP_LOCATION_DISP, L.LOCATION_DISPLAY_NAME AS TEMP_LOCATION "
            + "FROM BIB_MFHD BM, MFHD_ITEM MI, ITEM I LEFT OUTER JOIN LOCATION L ON I.TEMP_LOCATION = L.LOCATION_ID "
            + "WHERE BM.MFHD_ID = MI.MFHD_ID AND MI.ITEM_ID = I.ITEM_ID "
            + "AND BIB_ID > "+lowBib+" AND BIB_ID < "+highBib+" "
            + "ORDER BY BIB_ID, MFHD_ID, ITEM_ID";
    private static String[] itemIdFields = {"BIB_ID", "MFHD_ID", "ITEM_ID"};
    private static String hldgSql = "SELECT BM.BIB_ID, MM.MFHD_ID, MM.DISPLAY_CALL_NO, MM.NORMALIZED_CALL_NO, MM.CREATE_DATE AS HOLD_CREATE_DATE, MAX(ACTION_DATE) AS LAST_HOLD_UPDATE, CALL_NO_TYPE, "
            + "L.LOCATION_NAME AS PERM_LOCATION_DISP, L.LOCATION_DISPLAY_NAME AS PERM_LOCATION "
            + "FROM MFHD_MASTER MM, BIB_MFHD BM, MFHD_HISTORY MH, LOCATION L "
            + "WHERE MM.MFHD_ID = BM.MFHD_ID AND MH.MFHD_ID = BM.MFHD_ID AND MM.SUPPRESS_IN_OPAC = 'N' AND MM.LOCATION_ID = L.LOCATION_ID "
            + "AND BIB_ID > "+lowBib+" AND BIB_ID < "+highBib+" "
            + "GROUP BY BM.BIB_ID, MM.MFHD_ID, MM.DISPLAY_CALL_NO, MM.NORMALIZED_CALL_NO, MM.CREATE_DATE, CALL_NO_TYPE, L.LOCATION_NAME, L.LOCATION_DISPLAY_NAME "
            + "ORDER BY BIB_ID, MFHD_ID";
    private static String[] hldgIdFields = {"BIB_ID", "MFHD_ID"};
    private static String itemStatusSql = "SELECT BM.BIB_ID, MI.MFHD_ID, I.ITEM_ID, IST.ITEM_STATUS_TYPE AS STATUS_ID, IST.ITEM_STATUS_DESC STATUS "
            + "FROM BIB_MFHD BM, MFHD_ITEM MI, ITEM I, ITEM_STATUS, ITEM_STATUS_TYPE IST "
            + "WHERE BM.MFHD_ID = MI.MFHD_ID AND MI.ITEM_ID = I.ITEM_ID AND I.ITEM_ID = ITEM_STATUS.ITEM_ID AND ITEM_STATUS.ITEM_STATUS = IST.ITEM_STATUS_TYPE "
            + "AND BIB_ID > "+lowBib+" AND BIB_ID < "+highBib+" "
            + "ORDER BY BIB_ID, MFHD_ID, ITEM_ID, STATUS_ID";
    private static String[] itemStatusIdFields = {"BIB_ID", "MFHD_ID", "ITEM_ID", "STATUS_ID"};
    private final IntegratorOutputNode rootOutputNode = new IntegratorOutputNode(null);

    public static void main(String[] args) throws ParserConfigurationException, SAXException, FileNotFoundException, IOException, TransformerConfigurationException, TransformerException, ConnectionException {
        IntegratorSAX instance = new IntegratorSAX();
        instance.setupAndRun();
    }

    private void setupAndRun() throws ParserConfigurationException, SAXException, FileNotFoundException, TransformerConfigurationException, TransformerException, ConnectionException {
        String inputFileFolder = "too_much_skipping";
        File marcFile = new File("inputFiles/"+inputFileFolder+"/marc.xml");
        File hldgFile = new File("inputFiles/"+inputFileFolder+"/hldg.xml");
        File itemFile = new File("inputFiles/"+inputFileFolder+"/item.xml");
        File itemStatusFile = new File("inputFiles/"+inputFileFolder+"/item_status.xml");
        StatefulXMLFilter marcSxf = new StatefulXMLFilter();
        StatefulXMLFilter hldgSxf = new StatefulXMLFilter();
        StatefulXMLFilter itemSxf = new StatefulXMLFilter();
        StatefulXMLFilter itemStatusSxf = new StatefulXMLFilter();
        boolean fromDatabase = true;
        if (fromDatabase) {
            BinaryMARCXMLReader bmxr = new BinaryMARCXMLReader();
            bmxr.setHost(host);
            bmxr.setSid(sid);
            bmxr.setUser(user);
            bmxr.setPwd(pwd);
            bmxr.setSql(marcSql);
            bmxr.setIdFieldLabels(marcIdFields);
            bmxr.setOutputFieldLabels(marcOutputFields);
            marcSxf.setParent(bmxr);

            RSXMLReader hxr = new RSXMLReader();
            hxr.setHost(host);
            hxr.setSid(sid);
            hxr.setUser(user);
            hxr.setPwd(pwd);
            hxr.setSql(hldgSql);
            hxr.setIdFieldLabels(hldgIdFields);
            hldgSxf.setParent(hxr);

            RSXMLReader ixr = new RSXMLReader();
            ixr.setHost(host);
            ixr.setSid(sid);
            ixr.setUser(user);
            ixr.setPwd(pwd);
            ixr.setSql(itemSql);
            ixr.setIdFieldLabels(itemIdFields);
            itemSxf.setParent(ixr);

            RSXMLReader isxr = new RSXMLReader();
            isxr.setHost(host);
            isxr.setSid(sid);
            isxr.setUser(user);
            isxr.setPwd(pwd);
            isxr.setSql(itemStatusSql);
            isxr.setIdFieldLabels(itemStatusIdFields);
            itemStatusSxf.setParent(isxr);

        } else {
            // From file
            SAXParserFactory spf = SAXParserFactory.newInstance();
            spf.setNamespaceAware(true);
            marcSxf.setParent(spf.newSAXParser().getXMLReader());
            marcSxf.setInputSource(new InputSource(new FileInputStream(marcFile)));
            hldgSxf.setParent(spf.newSAXParser().getXMLReader());
            hldgSxf.setInputSource(new InputSource(new FileInputStream(hldgFile)));
            itemSxf.setParent(spf.newSAXParser().getXMLReader());
            itemSxf.setInputSource(new InputSource(new FileInputStream(itemFile)));
            itemStatusSxf.setParent(spf.newSAXParser().getXMLReader());
            itemStatusSxf.setInputSource(new InputSource(new FileInputStream(itemStatusFile)));
        }
        TransformerFactory tf = TransformerFactory.newInstance(TRANSFORMER_FACTORY_CLASS_NAME, null);
        Transformer t = tf.newTransformer();
        t.setOutputProperty(OutputKeys.INDENT, "yes");

        IntegratorOutputNode recordNode = new IntegratorOutputNode(null);
        IntegratorOutputNode hldgsNode = new IntegratorOutputNode(null);
        IntegratorOutputNode hldgNode = new IntegratorOutputNode(hldgSxf);
        IntegratorOutputNode itemsNode = new IntegratorOutputNode(null);
        IntegratorOutputNode itemNode = new IntegratorOutputNode(itemSxf);
        IntegratorOutputNode itemStatusesNode = new IntegratorOutputNode(null);
        IntegratorOutputNode itemStatusNode = new IntegratorOutputNode(itemStatusSxf);
        hldgsNode.addChild("hldg", hldgNode, false);
        recordNode.addChild("marc", new IntegratorOutputNode(marcSxf), false);
        recordNode.addChild("hldgs", hldgsNode, false);
        hldgNode.addChild("items", itemsNode, false);
        itemsNode.addChild("item", itemNode, false);
        itemNode.addChild("itemStatuses", itemStatusesNode, false);
        itemStatusesNode.addChild("itemStatus", itemStatusNode, false);

        rootOutputNode.addChild("record", recordNode, false);
        //rootOutputNode.setAggregating(false);
        long start = System.currentTimeMillis();
        boolean raw = false;
        if (!raw) {
            FileOutputStream fos = new FileOutputStream("/tmp/blah.xml");
            BufferedOutputStream bos = new BufferedOutputStream(fos);
            t.transform(new SAXSource(rootOutputNode, new InputSource()), new StreamResult(bos));
        } else {
            BufferingXMLFilter rawOutput = new BufferingXMLFilter();
            rootOutputNode.setContentHandler(rawOutput);
            try {
                rootOutputNode.run();
            } catch (Exception e) {
                e.printStackTrace(System.out);
            }
            rawOutput.play(null);
        }
        System.out.println("sax integrator duration: "+(System.currentTimeMillis() - start));
    }
    public static final String TRANSFORMER_FACTORY_CLASS_NAME = "net.sf.saxon.TransformerFactoryImpl";
}
