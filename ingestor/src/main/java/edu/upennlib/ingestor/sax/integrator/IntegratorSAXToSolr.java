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

package edu.upennlib.ingestor.sax.integrator;

import edu.upennlib.ingestor.sax.solr.SAXSolrPoster;
import edu.upennlib.ingestor.sax.utils.ConnectionException;
import edu.upennlib.ingestor.sax.xsl.JoiningXMLFilter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.sax.SAXResult;
import org.apache.solr.client.solrj.impl.StreamingUpdateSolrServer;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 *
 * @author michael
 */
public class IntegratorSAXToSolr {
    private static final boolean limitRange = true;
    private static final String lowBib = "3000000";
    private static final String highBib = "3010000";
    private static String host = "[host_or_ip]";
    private static String sid = "[sid]";
    private static String user = "[username]";
    private static String pwd = "[password]";
    private static String marcSql = "SELECT BIB_DATA.BIB_ID, BIB_DATA.SEQNUM, BIB_DATA.RECORD_SEGMENT "
            + "FROM PENNDB.BIB_DATA, BIB_MASTER "
            + "WHERE BIB_DATA.BIB_ID = BIB_MASTER.BIB_ID AND  BIB_MASTER.SUPPRESS_IN_OPAC = 'N' "
            + (limitRange ? "AND BIB_DATA.BIB_ID > "+lowBib+" AND BIB_DATA.BIB_ID < "+highBib+" " : "")
            + "ORDER BY 1, 2";
    private static String[] marcIdFields = {"BIB_ID"};
    private static String[] marcOutputFields = {"RECORD_SEGMENT"};
    private static String itemSql = "SELECT BM.BIB_ID, MI.MFHD_ID, I.ITEM_ID, MI.ITEM_ENUM, I.CREATE_DATE AS ITEM_CREATE_DATE, I.HOLDS_PLACED AS NUM_HOLDS, "
            + "L.LOCATION_NAME AS TEMP_LOCATION_DISP, L.LOCATION_DISPLAY_NAME AS TEMP_LOCATION "
            + "FROM BIB_MFHD BM, MFHD_ITEM MI, ITEM I LEFT OUTER JOIN LOCATION L ON I.TEMP_LOCATION = L.LOCATION_ID "
            + "WHERE BM.MFHD_ID = MI.MFHD_ID AND MI.ITEM_ID = I.ITEM_ID "
            + (limitRange ? "AND BIB_ID > "+lowBib+" AND BIB_ID < "+highBib+" " : "")
            + "ORDER BY 1, 2, 3";
    private static String[] itemIdFields = {"BIB_ID", "MFHD_ID", "ITEM_ID"};
    private static String hldgSql = "SELECT BM.BIB_ID, MM.MFHD_ID, MM.DISPLAY_CALL_NO, MM.NORMALIZED_CALL_NO, MM.CREATE_DATE AS HOLD_CREATE_DATE, MAX(ACTION_DATE) AS LAST_HOLD_UPDATE, CALL_NO_TYPE, "
            + "L.LOCATION_NAME AS PERM_LOCATION_DISP, L.LOCATION_DISPLAY_NAME AS PERM_LOCATION "
            + "FROM MFHD_MASTER MM, BIB_MFHD BM, MFHD_HISTORY MH, LOCATION L "
            + "WHERE MM.MFHD_ID = BM.MFHD_ID AND MH.MFHD_ID = BM.MFHD_ID AND MM.SUPPRESS_IN_OPAC = 'N' AND MM.LOCATION_ID = L.LOCATION_ID "
            + (limitRange ? "AND BIB_ID > "+lowBib+" AND BIB_ID < "+highBib+" " : "")
            + "GROUP BY BM.BIB_ID, MM.MFHD_ID, MM.DISPLAY_CALL_NO, MM.NORMALIZED_CALL_NO, MM.CREATE_DATE, CALL_NO_TYPE, L.LOCATION_NAME, L.LOCATION_DISPLAY_NAME "
            + "ORDER BY 1, 2";
    private static String[] hldgIdFields = {"BIB_ID", "MFHD_ID"};
    private static String itemStatusSql = "SELECT BM.BIB_ID, MI.MFHD_ID, I.ITEM_ID, IST.ITEM_STATUS_TYPE AS STATUS_ID, IST.ITEM_STATUS_DESC STATUS "
            + "FROM BIB_MFHD BM, MFHD_ITEM MI, ITEM I, ITEM_STATUS, ITEM_STATUS_TYPE IST "
            + "WHERE BM.MFHD_ID = MI.MFHD_ID AND MI.ITEM_ID = I.ITEM_ID AND I.ITEM_ID = ITEM_STATUS.ITEM_ID AND ITEM_STATUS.ITEM_STATUS = IST.ITEM_STATUS_TYPE "
            + (limitRange ? "AND BIB_ID > "+lowBib+" AND BIB_ID < "+highBib+" " : "")
            + "ORDER BY 1, 2, 3, 4";
    private static String[] itemStatusIdFields = {"BIB_ID", "MFHD_ID", "ITEM_ID", "STATUS_ID"};
    private final IntegratorOutputNode rootOutputNode = new IntegratorOutputNode();

    public static void main(String[] args) throws ParserConfigurationException, SAXException, FileNotFoundException, IOException, TransformerConfigurationException, TransformerException, ConnectionException {
        IntegratorSAXToSolr instance = new IntegratorSAXToSolr();
        instance.setupAndRun();
    }

    private void setupAndRun() throws ParserConfigurationException, SAXException, FileNotFoundException, TransformerConfigurationException, TransformerException, ConnectionException, IOException {
        String inputFileFolder = "too_much_skipping";
        File marcFile = new File("inputFiles/"+inputFileFolder+"/marc.xml");
        File hldgFile = new File("inputFiles/"+inputFileFolder+"/hldg.xml");
        File itemFile = new File("inputFiles/"+inputFileFolder+"/item.xml");
        File itemStatusFile = new File("inputFiles/"+inputFileFolder+"/item_status.xml");
        StatefulXMLFilter marcSxf = new StatefulXMLFilter();
        StatefulXMLFilter hldgSxf = new StatefulXMLFilter();
        StatefulXMLFilter itemSxf = new StatefulXMLFilter();
        StatefulXMLFilter itemStatusSxf = new StatefulXMLFilter();
        BinaryMARCXMLReader bmxr = null;
        RSXMLReader hxr = null;
        RSXMLReader ixr = null;
        RSXMLReader isxr = null;
        boolean fromDatabase = false;
        if (fromDatabase) {
            bmxr = new BinaryMARCXMLReader();
            bmxr.setHost(host);
            bmxr.setSid(sid);
            bmxr.setUser(user);
            bmxr.setPwd(pwd);
            bmxr.setSql(marcSql);
            bmxr.setIdFieldLabels(marcIdFields);
            bmxr.setOutputFieldLabels(marcOutputFields);

            hxr = new RSXMLReader();
            hxr.setHost(host);
            hxr.setSid(sid);
            hxr.setUser(user);
            hxr.setPwd(pwd);
            hxr.setSql(hldgSql);
            hxr.setIdFieldLabels(hldgIdFields);

            ixr = new RSXMLReader();
            ixr.setHost(host);
            ixr.setSid(sid);
            ixr.setUser(user);
            ixr.setPwd(pwd);
            ixr.setSql(itemSql);
            ixr.setIdFieldLabels(itemIdFields);

            isxr = new RSXMLReader();
            isxr.setHost(host);
            isxr.setSid(sid);
            isxr.setUser(user);
            isxr.setPwd(pwd);
            isxr.setSql(itemStatusSql);
            isxr.setIdFieldLabels(itemStatusIdFields);

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
        
        if (fromDatabase) {
            rootOutputNode.addDescendent("/record/marc", bmxr, true);
            rootOutputNode.addDescendent("/record/holdings/holding", hxr, false);
            rootOutputNode.addDescendent("/record/holdings/holding/items/item", ixr, false);
            rootOutputNode.addDescendent("/record/holdings/holding/items/item/itemStatuses/itemStatus", isxr, false);
        } else {
            rootOutputNode.addDescendent("/record/marc", marcSxf, true);
            rootOutputNode.addDescendent("/record/holdings/holding", hldgSxf, false);
            rootOutputNode.addDescendent("/record/holdings/holding/items/item", itemSxf, false);
            rootOutputNode.addDescendent("/record/holdings/holding/items/item/itemStatuses/itemStatus", itemStatusSxf, false);
        }
        long start = System.currentTimeMillis();
        JoiningXMLFilter joiner = new JoiningXMLFilter();
        SAXSolrPoster poster = setupSolr();
        joiner.transform(rootOutputNode, new InputSource(), new File("inputFiles/fullTest.xsl"), new SAXResult(poster));
        System.out.println("sax integrator duration: " + (System.currentTimeMillis() - start));
    }

    private SAXSolrPoster setupSolr() {
        String solrUrl = "http://[host_or_ip]:8082/franklin";
        int queueSize = 20;
        int threadCount = 3;
        SAXSolrPoster poster = new SAXSolrPoster();
        try {
            poster.setServer(new StreamingUpdateSolrServer(solrUrl, queueSize, threadCount));
        } catch (MalformedURLException ex) {
            throw new RuntimeException(ex);
        }
        return poster;
    }

    public static final String TRANSFORMER_FACTORY_CLASS_NAME = "net.sf.saxon.TransformerFactoryImpl";

}
