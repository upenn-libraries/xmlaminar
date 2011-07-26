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
package edu.upennlib.ingestor.sax.xsl;

import java.util.LinkedList;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.xml.sax.ext.LexicalHandler;

/**
 *
 * @author michael
 */
public class SaxEventExecutor {

    private static final int CHAR_ARRAY_SIZE = 16384;
    private static final LinkedList<char[]> charArrays = new LinkedList<char[]>();
    private char[] lastCharArrayWritten = null;

    public static char[] getNextCharArray() {
        return getNextCharArray(CHAR_ARRAY_SIZE);
    }

    public static char[] getNextCharArray(int minimumSize) {
        char[] next;
        synchronized (charArrays) {
            if (charArrays.isEmpty()) {
                return new char[CHAR_ARRAY_SIZE];
            } else {
                next = charArrays.remove();
            }
        }
        if (next.length < minimumSize) {
            // discards next -- this will probably never happen, but avoids possible buffer overflows, etc.
            return new char[minimumSize];
        } else {
            return next;
        }
    }

    public static String eventToString(Object[] event) {
        StringBuilder sb = new StringBuilder();
        switch ((SaxEventType)event[0]) {
            case characters:
                sb.append("[characters, ");
                sb.append((char[])event[1], (Integer)event[2], (Integer)event[3]).append(']');
                break;
            case startElement:
                sb.append("[startElement, ");
                sb.append((String)event[1]).append(", ");
                sb.append((String)event[2]).append(", ");
                sb.append((String)event[3]);
                sb.append(attsToString((Attributes)event[4])).append(']');
                break;
            default:
        }
        return sb.toString();
    }
    
    private static String attsToString(Attributes atts) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < atts.getLength(); i++) {
            if (i == 0) {
                sb.append(", ").append(atts.getClass().getSimpleName()).append('[');
            }
            sb.append(atts.getURI(i)).append(atts.getQName(i)).append("=\"").append(atts.getValue(i));
            if (i < atts.getLength() - 1) {
                sb.append("\", ");
            } else {
                sb.append(']');
            }
        }
        return sb.toString();
    }


    private void checkFreeCharArray(Object[] args) {
        char[] ch = (char[]) args[1];
        if (ch != lastCharArrayWritten) {
            if (lastCharArrayWritten != null) {
                synchronized (charArrays) {
                    charArrays.add(lastCharArrayWritten);
                }
            }
            lastCharArrayWritten = ch;
        }
    }

    public int executeSaxEvent(ContentHandler ch, Object[] args, boolean writeStructural, boolean writeNonStructural) throws SAXException {
        int level = 0;
        switch ((SaxEventType) args[0]) {
            case characters:
                if (writeNonStructural) {
                    ch.characters((char[]) args[1], (Integer) args[2], (Integer) args[3]);
                }
                //XXX checkFreeCharArray(args);
                break;
            case endDocument:
                if (writeStructural) {
                    ch.endDocument();
                }
                break;
            case endElement:
                if (writeStructural) {
                    ch.endElement((String) args[1], (String) args[2], (String) args[3]);
                    level--;
                }
                break;
            case endPrefixMapping:
                if (writeStructural) {
                    ch.endPrefixMapping((String) args[1]);
                }
                break;
            case ignorableWhitespace:
                if (writeNonStructural) {
                    ch.ignorableWhitespace((char[]) args[1], (Integer) args[2], (Integer) args[3]);
                }
                //XXX checkFreeCharArray(args);
                break;
            case processingInstruction:
                if (writeNonStructural) {
                    ch.processingInstruction((String) args[1], (String) args[2]);
                }
                break;
            case skippedEntity:
                if (writeNonStructural) {
                    ch.skippedEntity((String) args[1]);
                }
                break;
            case startDocument:
                if (writeStructural) {
                    ch.startDocument();
                }
                break;
            case startElement:
                if (writeStructural) {
                    ch.startElement((String) args[1], (String) args[2], (String) args[3], (Attributes) args[4]);
                    level++;
                }
                break;
            case startPrefixMapping:
                if (writeStructural) {
                    ch.startPrefixMapping((String) args[1], (String) args[2]);
                }
                break;
            case startCDATA:
                System.out.println("blah");
                if (writeStructural && ch instanceof LexicalHandler) {
                    ((LexicalHandler)ch).startCDATA();
                }
                break;
            case startDTD:
                if (writeStructural && ch instanceof LexicalHandler) {
                    ((LexicalHandler)ch).startDTD((String) args[1], (String) args[2], (String) args[3]);
                }
                break;
            case startEntity:
                if (writeStructural && ch instanceof LexicalHandler) {
                    ((LexicalHandler)ch).startEntity((String) args[1]);
                }
                break;
            case endCDATA:
                if (writeStructural && ch instanceof LexicalHandler) {
                    ((LexicalHandler)ch).endCDATA();
                }
                break;
            case endDTD:
                if (writeStructural && ch instanceof LexicalHandler) {
                    ((LexicalHandler)ch).endDTD();
                }
                break;
            case endEntity:
                if (writeStructural && ch instanceof LexicalHandler) {
                    ((LexicalHandler)ch).endEntity((String) args[1]);
                }
                break;
        }
        return level;
    }

    public static boolean isStructurallySignificant(Object[] event) {
        switch ((SaxEventType) event[0]) {
            case startDocument:
            case startElement:
            case startPrefixMapping:
            case endDocument:
            case endElement:
            case endPrefixMapping:
                return true;
            default:
                return false;
        }
    }
    public static boolean equals(Object[] one, Object[] two) {
        if (one.length != two.length) {
            return false;
        }
        for (int i = 0; i < one.length; i++) {
            if (!one[i].equals(two[i])) {
                if (one[i] instanceof Attributes) {
                    Attributes attOne = (Attributes) one[i];
                    Attributes attTwo = (Attributes) two[i];
                    if (attOne.getLength() != attTwo.getLength()) {
                        return false;
                    }
                    for (int j = 0; j < attOne.getLength(); j++) {
                        if (!attOne.getURI(j).equals(attTwo.getURI(j))) {
                            return false;
                        }
                        if (!attOne.getLocalName(j).equals(attTwo.getLocalName(j))) {
                            return false;
                        }
                        if (!attOne.getQName(j).equals(attTwo.getQName(j))) {
                            return false;
                        }
                        if (!attOne.getType(j).equals(attTwo.getType(j))) {
                            return false;
                        }
                        if (!attOne.getValue(j).equals(attTwo.getValue(j))) {
                            return false;
                        }
                    }
                } else {
                    return false;
                }
            }
        }
        return true;
    }
}
