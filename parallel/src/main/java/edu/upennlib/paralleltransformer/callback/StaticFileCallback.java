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
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package edu.upennlib.paralleltransformer.callback;

import edu.upennlib.xmlutils.VolatileSAXSource;
import java.io.File;
import java.io.IOException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.sax.SAXSource;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLFilter;
import org.xml.sax.XMLReader;

/**
 *
 * @author magibney
 */
public class StaticFileCallback implements XMLReaderCallback {
    private final File staticFile;
    private final Transformer t;
    private final XMLFilter outputFilter;
    private final boolean gzipOutput;

    public StaticFileCallback(Transformer t, File staticFile, XMLFilter outputFilter, boolean gzipOutput) {
        this.staticFile = staticFile;
        this.t = t;
        this.outputFilter = outputFilter;
        this.gzipOutput = gzipOutput;
    }

    public StaticFileCallback(Transformer t, File staticFile) {
        this(t, staticFile, null, false);
    }

    public StaticFileCallback(File staticFile) throws TransformerConfigurationException {
        this(TransformerFactory.newInstance().newTransformer(), staticFile);
    }

    @Override
    public void callback(VolatileSAXSource source) throws SAXException, IOException {
        if (outputFilter != null) {
            outputFilter.setParent(source.getXMLReader());
            source.setXMLReader(outputFilter);
        }
        StreamCallback.writeToFile(source, staticFile, t, gzipOutput);
    }

    @Override
    public void finished(Throwable t) {
    }
    
}
