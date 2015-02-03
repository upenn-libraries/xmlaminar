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
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.StringReader;
import java.util.zip.GZIPOutputStream;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.stream.StreamResult;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLFilter;
import org.xml.sax.XMLReader;

/**
 *
 * @author magibney
 */
public class StdoutCallback implements XMLReaderCallback {
    private static final boolean DEFAULT_GZIP_OUTPUT = false;
    private final Transformer t;
    private final XMLFilter outputFilter;
    private final boolean gzipOutput;
    private OutputStream out;

    public StdoutCallback(Transformer t, XMLFilter outputFilter, boolean gzipOutput) {
        this.t = t;
        this.outputFilter = outputFilter;
        this.gzipOutput = gzipOutput;
    }

    public StdoutCallback(Transformer t) {
        this(t, null, false);
    }

    public StdoutCallback(XMLFilter outputFilter) throws TransformerConfigurationException {
        this(TransformerFactory.newInstance().newTransformer(), outputFilter, DEFAULT_GZIP_OUTPUT);
    }

    public StdoutCallback() throws TransformerConfigurationException {
        this(TransformerFactory.newInstance().newTransformer(), null, DEFAULT_GZIP_OUTPUT);
    }

    @Override
    public void callback(VolatileSAXSource source) throws SAXException, IOException {
        if (outputFilter != null) {
            outputFilter.setParent(source.getXMLReader());
            source.setXMLReader(outputFilter);
        }
        if (out == null) {
            out = gzipOutput ? new GZIPOutputStream(System.out) : System.out;
        }
        StreamCallback.writeToStream(source, new StreamResult(out), t);
    }

    @Override
    public void finished(Throwable t) {
        try {
            if (gzipOutput) {
                ((GZIPOutputStream) out).finish();
            }
            out.flush();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }
    
}
