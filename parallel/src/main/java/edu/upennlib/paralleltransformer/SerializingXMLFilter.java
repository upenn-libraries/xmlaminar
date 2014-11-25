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

package edu.upennlib.paralleltransformer;

import java.io.IOException;
import javax.xml.transform.Result;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.sax.SAXTransformerFactory;
import javax.xml.transform.sax.TransformerHandler;
import javax.xml.transform.stream.StreamResult;
import net.sf.saxon.Controller;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXNotSupportedException;
import org.xml.sax.helpers.XMLFilterImpl;

/**
 *
 * @author magibney
 */
public class SerializingXMLFilter extends XMLFilterImpl {
    
    private static final Logger LOG = LoggerFactory.getLogger(SerializingXMLFilter.class);
    private final TransformerHandler th;
    private final Result res;
    
    public SerializingXMLFilter(Result res) {
        SAXTransformerFactory stf = (SAXTransformerFactory) TransformerFactory.newInstance("net.sf.saxon.TransformerFactoryImpl", null);
        try {
            th = stf.newTransformerHandler();
        } catch (TransformerConfigurationException ex) {
            throw new RuntimeException(ex);
        }
        this.res = res;
    }

    @Override
    public void parse(String systemId) throws SAXException, IOException {
        setupParse();
        super.parse(systemId);
    }

    @Override
    public void parse(InputSource input) throws SAXException, IOException {
        setupParse();
        super.parse(input);
    }
    
    private void setupParse() {
        Transformer t = th.getTransformer();
        t.reset();
        if (t instanceof Controller) {
            ((Controller) t).clearDocumentPool();
        }
        try {
            setProperty(TXMLFilter.OUTPUT_TRANSFORMER_PROPERTY_NAME, t);
        } catch (SAXNotRecognizedException ex) {
            LOG.info("ignoring setProperty({})", TXMLFilter.OUTPUT_TRANSFORMER_PROPERTY_NAME);
        } catch (SAXNotSupportedException ex) {
            LOG.info("ignoring setProperty({})", TXMLFilter.OUTPUT_TRANSFORMER_PROPERTY_NAME);
        }
        th.setResult(res);
        setContentHandler(th);
    }
    
    
    
}
