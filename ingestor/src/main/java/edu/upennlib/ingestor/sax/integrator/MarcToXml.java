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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.sax.SAXTransformerFactory;
import javax.xml.transform.sax.TransformerHandler;
import javax.xml.transform.stream.StreamResult;
import org.marc4j.MarcXmlReader;

/**
 *
 * @author michael
 */
public class MarcToXml {

    static Connection connection;
    static String sql = "SELECT DISTINCT BIB_DATA.BIB_ID, BIB_DATA.SEQNUM, BIB_DATA.RECORD_SEGMENT "
            + "FROM PENNDB.BIB_DATA, BIB_MASTER "
            + "WHERE BIB_DATA.BIB_ID = BIB_MASTER.BIB_ID AND  BIB_MASTER.SUPPRESS_IN_OPAC = 'N' "
            + "AND BIB_DATA.BIB_ID = 3000001"
            + "ORDER BY BIB_ID, SEQNUM";
    static String host = "[host_or_ip]";
    static String sid = "[sid]";
    static String user = "[username]";
    static String pwd = "[password]";


    public static void main(String args[]) throws ConnectionException, SQLException, FileNotFoundException, IOException {
        connection = new Connection();
        connection.setHost(host);
        connection.setPwd(pwd);
        connection.setSid(sid);
        connection.setSql(sql);
        connection.setUser(user);
        ResultSet rs = connection.getResultSet();
        FileOutputStream fos = new FileOutputStream("inputFiles/marc/1.mrc");
        while (rs.next()) {
            InputStream binaryStream = rs.getBinaryStream("RECORD_SEGMENT");
            int next = -1;
            while ((next = binaryStream.read()) != -1) {
                fos.write(next);
            }
            binaryStream.close();
        }
        fos.close();
        rs.close();
    }
}
