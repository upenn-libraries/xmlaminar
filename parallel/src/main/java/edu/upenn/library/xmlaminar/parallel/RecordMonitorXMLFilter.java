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

package edu.upenn.library.xmlaminar.parallel;

import edu.upenn.library.xmlaminar.VolatileXMLFilterImpl;
import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;
import org.xml.sax.helpers.XMLFilterImpl;

/**
 *
 * @author magibney
 */
public abstract class RecordMonitorXMLFilter extends VolatileXMLFilterImpl {

    private int level = -1;
    private int seekLevel = 0;
    
    private static class Condition {
        private final String uri;
        private final String localName;
        private final Attributes requiredAtts;
        
        private Condition(String uri, String localName, Attributes requiredAtts) {
            this.uri = uri;
            this.localName = localName;
            this.requiredAtts = requiredAtts;
        }
        
        public boolean satisfiedBy(String uri, String localName, Attributes atts) {
            if ((this.uri == null || this.uri.equals(uri)) && (this.localName == null || this.localName.equals(localName))) {
                if (requiredAtts == null || requiredAtts.getLength() < 1) {
                    return true;
                } else {
                    for (int i = 0; i < requiredAtts.getLength(); i++) {
                        String u = requiredAtts.getURI(i);
                        String l = requiredAtts.getLocalName(i);
                        if (atts.getIndex(u, l) < 0) {
                            return false;
                        }
                    }
                    return true;
                }
            } else {
                return false;
            }
        }
        
    }
    
    private final Condition[] conditions;
    private final String outputAttributeURI;
    private final String outputAttribute;
    
    protected RecordMonitorXMLFilter(RecordMonitorXMLFilter prototype) {
        this.conditions = prototype.conditions;
        this.outputAttributeURI = prototype.outputAttributeURI;
        this.outputAttribute = prototype.outputAttribute;
    }
    
    protected RecordMonitorXMLFilter(String xpath) {
        Matcher m = XPATH_PARSE.matcher(xpath);
        ArrayList<Condition> condList = new ArrayList<Condition>();
        String outURI = null;
        String outLocal = null;
        while (m.find()) {
            String element = m.group(ELEMENT_INDEX);
            String reqAttName = m.group(REQ_ATT_NAME_INDEX);
            String reqAttValue = m.group(REQ_ATT_VALUE_INDEX);
            String outAttName = m.group(OUT_ATT_QNAME_INDEX);
            String[] parsedQName = new String[2];
            if (outAttName != null) {
                if (!m.hitEnd()) {
                    throw new IllegalArgumentException("premature output attribute spec: " + xpath);
                }
                parseQName(outAttName, parsedQName);
                outURI = parsedQName[0];
                outLocal = parsedQName[1];
            } else {
                AttributesImpl atts = null;
                if (reqAttName != null) {
                    if (reqAttValue == null) {
                        throw new IllegalArgumentException("must specify attribute value for "+reqAttName);
                    }
                    parseQName(reqAttName, parsedQName);
                    atts = new AttributesImpl();
                    atts.addAttribute(parsedQName[0], parsedQName[1], reqAttName, "CDATA", reqAttValue);
                }
                parseQName(element, parsedQName);
                String elementURI = parsedQName[0];
                String elementLocal = parsedQName[1];
                if ("*".equals(elementURI)) {
                    elementURI = null;
                }
                if ("*".equals(elementLocal)) {
                    elementLocal = null;
                }
                condList.add(new Condition(elementURI, elementLocal, atts));
            }
        }
        if (!m.hitEnd()) {
            throw new IllegalArgumentException("failed to completely parse xpath: "+xpath);
        }
        outputAttributeURI = outURI == null ? "" : outURI;
        outputAttribute = outLocal;
        conditions = condList.toArray(new Condition[condList.size()]);
    }
    
    private static String[] parseQName(String qName, String[] ret) {
        int delimIndex = qName.indexOf(':');
        if (delimIndex < 0) {
            ret[0] = null;
            ret[1] = qName;
        } else {
            ret[0] = qName.substring(0, delimIndex);
            ret[1] = qName.substring(delimIndex + 1);
        }
        return ret;
    }
    
    private static final Pattern XPATH_PARSE = Pattern.compile("/(([^@/\\[]+)(\\[@([^= ]+) *= *\"([^\"]*)\" *\\])?)|@([^ ]+)");
    private static final int OUT_ATT_QNAME_INDEX = 6;
    private static final int ELEMENT_INDEX = 2;
    private static final int REQ_ATT_NAME_INDEX = 4;
    private static final int REQ_ATT_VALUE_INDEX = 5;
    
    protected void reset() {
        level = -1;
        seekLevel = 0;
        searchCharacters = false;
    }

    @Override
    public void parse(String systemId) throws SAXException, IOException {
        reset();
        super.parse(systemId);
    }

    @Override
    public void parse(InputSource input) throws SAXException, IOException {
        reset();
        super.parse(input);
    }
    
    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        if (searchCharacters) {
            sb.append(ch, start, length);
        }
        super.characters(ch, start, length);
    }
    
    private final StringBuilder sb = new StringBuilder();

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        if (searchCharacters) {
            register(sb.toString());
            searchCharacters = false;
        }
        super.endElement(uri, localName, qName);
        if (level < seekLevel) {
            seekLevel--;
        }
        level--;
    }

    private boolean searchCharacters = false;
    
    @Override
    public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
        level++;
        if (searchCharacters) {
            register(sb.toString());
            searchCharacters = false;
        } else if (level == seekLevel && level < conditions.length 
                && conditions[level].satisfiedBy(uri, localName, atts)) {
            seekLevel++;
            if (seekLevel == conditions.length) {
                if (outputAttribute == null) {
                    searchCharacters = true;
                    sb.setLength(0);
                } else {
                    register(atts.getValue(outputAttributeURI, outputAttribute));
                }
            }
        }
        super.startElement(uri, localName, qName, atts);
    }
    
    public abstract void register(String id) throws SAXException;
    
    public abstract String getRecordIdString();
    
    public abstract RecordMonitorXMLFilter newInstance();
    
}
