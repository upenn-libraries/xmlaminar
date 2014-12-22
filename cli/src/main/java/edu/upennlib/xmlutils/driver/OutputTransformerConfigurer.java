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

package edu.upennlib.xmlutils.driver;

import static edu.upennlib.paralleltransformer.TXMLFilter.OUTPUT_TRANSFORMER_PROPERTY_NAME;
import java.util.Map;
import javax.xml.transform.Transformer;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXNotSupportedException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLFilterImpl;

/**
 *
 * @author magibney
 */
public class OutputTransformerConfigurer extends XMLFilterImpl {
    
    private final Map<String, String> outputProperties;
    
    public OutputTransformerConfigurer() {
        this(null, null);
    }
    
    public OutputTransformerConfigurer(XMLReader parent) {
        this(parent, null);
    }
    
    public OutputTransformerConfigurer(Map<String, String> outputProperties) {
        this(null, outputProperties);
    }
    
    public OutputTransformerConfigurer(XMLReader parent, Map<String, String> outputProperties) {
        super(parent);
        this.outputProperties = outputProperties;
    }
    
    @Override
    public void setProperty(String name, Object value) throws SAXNotRecognizedException, SAXNotSupportedException {
        try {
            super.setProperty(name, value);
        } catch (SAXNotRecognizedException ex) {
            if (!OUTPUT_TRANSFORMER_PROPERTY_NAME.equals(name)) {
                throw ex;
            }
        } catch (SAXNotSupportedException ex) {
            if (!OUTPUT_TRANSFORMER_PROPERTY_NAME.equals(name)) {
                throw ex;
            }
        }
        if (OUTPUT_TRANSFORMER_PROPERTY_NAME.equals(name)) {
            configureOutputTransformer((Transformer) value);
        }
    }

    private void configureOutputTransformer(Transformer transformer) {
        if (outputProperties != null && !outputProperties.isEmpty()) {
            for (Map.Entry<String, String> e : outputProperties.entrySet()) {
                transformer.setOutputProperty(e.getKey(), e.getValue());
            }
        }
    }
    
}
