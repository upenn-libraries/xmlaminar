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

import java.io.IOException;
import java.io.Reader;
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
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;

/**
 *
 * @author michael
 */
public class RSXMLReader extends SQLXMLReader {

    char[] characters = new char[1024];
    AttributesImpl attRunner = new AttributesImpl();

    public RSXMLReader() {
        super(Reader.class);
    }

    @Override
    protected void outputFieldAsSAXEvents(long selfId, String fieldLabel, Object rawContent) throws SAXException, IOException {
        if (rawContent != null) {
            Reader content = (Reader) rawContent;
            attRunner.clear();
            ch.startElement("", fieldLabel, fieldLabel, attRunner);
            outputCharacters(content);
            ch.endElement("", fieldLabel, fieldLabel);
        }
    }

    private void outputCharacters(Reader content) throws IOException, SAXException {
        int length = -1;
        while ((length = content.read(characters)) != -1) {
            ch.characters(characters, 0, length);
        }
    }

    private static String host = "[host_or_ip]";
    private static String sid = "[sid]";
    private static String user = "[username]";
    private static String pwd = "[password]";
    private static String sql = "SELECT BM.BIB_ID, MI.MFHD_ID, I.ITEM_ID, MI.ITEM_ENUM, I.CREATE_DATE AS ITEM_CREATE_DATE, I.HOLDS_PLACED AS NUM_HOLDS, "
            + "L.LOCATION_NAME AS TEMP_LOCATION_DISP, L.LOCATION_DISPLAY_NAME AS TEMP_LOCATION "
            + "FROM BIB_MFHD BM, MFHD_ITEM MI, ITEM I LEFT OUTER JOIN LOCATION L ON I.TEMP_LOCATION = L.LOCATION_ID "
            + "WHERE BM.MFHD_ID = MI.MFHD_ID AND MI.ITEM_ID = I.ITEM_ID "
            + "AND BIB_ID > 3000000 AND BIB_ID < 3010000 "
            + "ORDER BY BIB_ID, MFHD_ID, ITEM_ID";

    public static void main(String[] args) throws TransformerConfigurationException, TransformerException, ConnectionException {

        RSXMLReader instance = new RSXMLReader();
        instance.logger.addAppender(new ConsoleAppender(new TTCCLayout(), "System.out"));
        instance.logger.setLevel(Level.WARN);
        instance.setHost(host);
        instance.setSid(sid);
        instance.setUser(user);
        instance.setPwd(pwd);
        instance.setSql(sql);
        String[] ifl = {"BIB_ID", "MFHD_ID", "ITEM_ID"};
        instance.setIdFieldLabels(ifl);

        SAXTransformerFactory stf = (SAXTransformerFactory)TransformerFactory.newInstance(TRANSFORMER_FACTORY_CLASS_NAME, null);
        Transformer t = stf.newTransformer();
        t.setOutputProperty(OutputKeys.INDENT, "yes");
        t.transform(new SAXSource(instance, new InputSource()), new StreamResult("/tmp/db_out.xml"));
    }

}
