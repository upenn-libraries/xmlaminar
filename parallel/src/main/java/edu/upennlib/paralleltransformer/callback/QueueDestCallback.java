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

import edu.upennlib.paralleltransformer.QueueSourceXMLFilter;
import edu.upennlib.xmlutils.VolatileSAXSource;
import java.io.IOException;
import javax.xml.transform.sax.SAXSource;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

/**
 *
 * @author magibney
 */
public class QueueDestCallback implements XMLReaderCallback {

    private final QueueSourceXMLFilter dest;
    
    public QueueDestCallback(QueueSourceXMLFilter dest) {
        this.dest = dest;
    }
    
    @Override
    public void callback(VolatileSAXSource source) throws SAXException, IOException {
        try {
            dest.getParseQueue().put(source);
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void finished(Throwable t) {
        try {
            dest.getParseQueue().put(dest.getFinishedSAXSource(t));
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
    }
    
}
