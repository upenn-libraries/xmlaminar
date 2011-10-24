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

package edu.upennlib.xmlutils.fsxml;

import edu.upennlib.xmlutils.Element;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.stream.StreamResult;
import org.xml.sax.InputSource;

/**
 *
 * @author michael
 */
public class FilesystemXMLReaderRepo extends FilesystemXMLReader {

    public static void main(String[] args) throws FileNotFoundException, TransformerConfigurationException, TransformerException, IOException {
        FilesystemXMLReader instance = new FilesystemXMLReaderRepo();
        instance.setFollowSymlinks(false);
        File rootFile = new File("/repo");
        BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream("/tmp/listingRepo.xml"));
        TransformerFactory tf = TransformerFactory.newInstance();
        Transformer t = tf.newTransformer();
        t.setOutputProperty(OutputKeys.INDENT, "yes");
        t.transform(new SAXSource(instance, new InputSource(rootFile.getAbsolutePath())), new StreamResult(bos));
        bos.close();
    }

    private static final String DFLAT_MARKER = "0=dflat";
    private static final Element root = new Element("repoRoot");

    public FilesystemXMLReaderRepo() {
        super(root, null);
    }

    @Override
    protected boolean startWriteOutput(FsxmlElement type, File f, File[] children) {
        if (type == FsxmlElement.dir) {
            for (File child : children) {
                String childName = child.getName();
                if (childName.startsWith(DFLAT_MARKER)) {
                    return true;
                } else if (childName.compareTo(DFLAT_MARKER) > 0) {
                    return false;
                }
            }
        }
        return false;
    }

}
