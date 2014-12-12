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

package edu.upennlib.paralleltransformer.callback;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.stream.StreamResult;

/**
 *
 * @author magibney
 */
public class StreamCallback {
    
    static void writeToFile(SAXSource source, File nextFile, Transformer t) throws FileNotFoundException, IOException {
        OutputStream out = new BufferedOutputStream(new FileOutputStream(nextFile));
        StreamResult res = new StreamResult(out);
        res.setSystemId(nextFile);
        try {
            writeToStream(source, res, t);
        } finally {
            out.close();
        }
    }

    static void writeToStream(SAXSource source, StreamResult out, Transformer t) throws FileNotFoundException, IOException {
        t.reset();
        try {
            t.transform(source, out);
        } catch (TransformerException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static int extensionStartIndex(String path) {
        if (path == null) {
            return -1;
        } else {
            int lastPathElementStart = path.lastIndexOf('/') + 1;
            String lastPathElement = path.substring(lastPathElementStart);
            int extStart = lastPathElement.lastIndexOf('.');
            if (extStart > 0) {
                return lastPathElementStart + extStart;
            } else {
                return -1;
            }
        }
    }
    
    public static String getExtension(String path) {
        int extStart = extensionStartIndex(path);
        if (extStart < 0 ) {
            return "";
        } else {
            return path.substring(extStart);
        }
    }
    
    public static String getBasename(String path) {
        int extStart = extensionStartIndex(path);
        if (extStart < 0 ) {
            return path;
        } else {
            return path.substring(0, extStart);
        }
    }
    
}
