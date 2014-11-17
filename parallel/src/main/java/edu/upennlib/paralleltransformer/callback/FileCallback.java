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
import org.xml.sax.InputSource;
import org.xml.sax.XMLReader;

/**
 *
 * @author magibney
 */
public class FileCallback {
    
    static void writeToFile(XMLReader reader, InputSource input, File nextFile, Transformer t) throws FileNotFoundException, IOException {
        t.reset();
        OutputStream out = new BufferedOutputStream(new FileOutputStream(nextFile));
        StreamResult res = new StreamResult(out);
        res.setSystemId(nextFile);
        try {
            t.transform(new SAXSource(reader, input), res);
        } catch (TransformerException ex) {
            throw new RuntimeException(ex);
        } finally {
            out.close();
        }
    }

}
