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

package edu.upennlib.xmlutils;

import java.io.IOException;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;

/**
 *
 * @author michael
 */
public abstract class JoiningXMLFilter extends XMLFilterLexicalHandlerImpl {

    public final int RECORD_LEVEL = 1;
    private int level = -1;
    private boolean encounteredFirstRecord = false;
    private boolean inPreRecord = true;
    private final UnboundedContentHandlerBuffer docLevelEventBuffer = new UnboundedContentHandlerBuffer();
    private final SaxEventVerifier preRecordEventVerifier = new SaxEventVerifier();

    @Override
    public abstract void parse(InputSource input) throws SAXException, IOException;

    @Override
    public abstract void parse(String systemId) throws SAXException, IOException;
        
    @Override
    public void startDocument() throws SAXException {
        inPreRecord = true;
        docLevelEventBuffer.clear();
        if (!encounteredFirstRecord) {
            preRecordEventVerifier.recordStart();
            super.startDocument();
        } else {
            preRecordEventVerifier.verifyStart();
            docLevelEventBuffer.startDocument();
        }
    }

    @Override
    public void endDocument() throws SAXException {
        docLevelEventBuffer.endDocument();
    }

    public void finished() throws SAXException {
        docLevelEventBuffer.play(getContentHandler(), lh);
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        if (level < RECORD_LEVEL) {
            if (inPreRecord) {
                preRecordEventVerifier.endElement(uri, localName, qName);
            }
            if (!encounteredFirstRecord) {
                super.endElement(uri, localName, qName);
            } else {
                docLevelEventBuffer.endElement(uri, localName, qName);
            }
        } else {
            if (level == RECORD_LEVEL) {
                docLevelEventBuffer.clear();
            }
            super.endElement(uri, localName, qName);
        }
        level--;
    }

    @Override
    public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
        level++;
        if (level < RECORD_LEVEL) {
            if (inPreRecord) {
                preRecordEventVerifier.startElement(uri, localName, qName, atts);
            }
            if (!encounteredFirstRecord) {
                super.startElement(uri, localName, qName, atts);
            } else {
                docLevelEventBuffer.startElement(uri, localName, qName, atts);
            }
        } else {
            if (level == RECORD_LEVEL) {
                if (!encounteredFirstRecord) {
                    preRecordEventVerifier.recordEnd();
                    encounteredFirstRecord = true;
                } else {
                    preRecordEventVerifier.verifyEnd();
                    if (!inPreRecord) {
                        level += docLevelEventBuffer.play(getContentHandler(), lh);
                    } else {
                        level += docLevelEventBuffer.playMostRecentStructurallyInsignificant(getContentHandler(), lh);
                    }
                }
                inPreRecord = false;
            }
            super.startElement(uri, localName, qName, atts);
        }
    }

    @Override
    public void setDocumentLocator(Locator locator) {
        docLevelEventBuffer.clear();
        super.setDocumentLocator(locator);
    }

    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        if (level < RECORD_LEVEL && encounteredFirstRecord) {
            docLevelEventBuffer.characters(ch, start, length);
        } else {
            super.characters(ch, start, length);
        }
    }

    @Override
    public void endPrefixMapping(String prefix) throws SAXException {
        if (level < RECORD_LEVEL) {
            if (inPreRecord) {
                preRecordEventVerifier.endPrefixMapping(prefix);
            }
            if (!encounteredFirstRecord) {
                super.endPrefixMapping(prefix);
            } else {
                docLevelEventBuffer.endPrefixMapping(prefix);
            }
        } else {
            super.endPrefixMapping(prefix);
        }
    }

    @Override
    public void ignorableWhitespace(char[] ch, int start, int length) throws SAXException {
        if (level < RECORD_LEVEL && encounteredFirstRecord) {
            docLevelEventBuffer.ignorableWhitespace(ch, start, length);
        } else {
            super.ignorableWhitespace(ch, start, length);
        }
    }

    @Override
    public void notationDecl(String name, String publicId, String systemId) throws SAXException {
        if (level < RECORD_LEVEL && encounteredFirstRecord) {
            docLevelEventBuffer.notationDecl(name, publicId, systemId);
        } else {
            super.notationDecl(name, publicId, systemId);
        }
    }

    @Override
    public void processingInstruction(String target, String data) throws SAXException {
        if (level < RECORD_LEVEL) {
            if (inPreRecord) {
                preRecordEventVerifier.processingInstruction(target, data);
            }
            if (!encounteredFirstRecord) {
                super.processingInstruction(target, data);
            } else {
                docLevelEventBuffer.processingInstruction(target, data);
            }
        } else {
            super.processingInstruction(target, data);
        }
    }

    @Override
    public void skippedEntity(String name) throws SAXException {
        if (level < RECORD_LEVEL && encounteredFirstRecord) {
            docLevelEventBuffer.skippedEntity(name);
        } else {
            super.skippedEntity(name);
        }
    }

    @Override
    public void startPrefixMapping(String prefix, String uri) throws SAXException {
        if (level < RECORD_LEVEL) {
            if (inPreRecord) {
                preRecordEventVerifier.startPrefixMapping(prefix, uri);
            }
            if (!encounteredFirstRecord) {
                super.startPrefixMapping(prefix, uri);
            } else {
                docLevelEventBuffer.startPrefixMapping(prefix, uri);
            }
        } else {
            super.startPrefixMapping(prefix, uri);
        }
    }

    @Override
    public void unparsedEntityDecl(String name, String publicId, String systemId, String notationName) throws SAXException {
        if (level < RECORD_LEVEL && encounteredFirstRecord) {
            docLevelEventBuffer.unparsedEntityDecl(name, publicId, systemId, notationName);
        } else {
            super.unparsedEntityDecl(name, publicId, systemId, notationName);
        }
    }

    @Override
    public void comment(char[] ch, int start, int length) throws SAXException {
        if (level < RECORD_LEVEL && encounteredFirstRecord) {
            docLevelEventBuffer.comment(ch, start, length);
        } else {
            super.comment(ch, start, length);
        }
    }

    @Override
    public void endCDATA() throws SAXException {
        if (level < RECORD_LEVEL && encounteredFirstRecord) {
            docLevelEventBuffer.endCDATA();
        } else {
            super.endCDATA();
        }
    }

    @Override
    public void endDTD() throws SAXException {
        if (level < RECORD_LEVEL && encounteredFirstRecord) {
            docLevelEventBuffer.endDTD();
        } else {
            super.endDTD();
        }
    }

    @Override
    public void endEntity(String name) throws SAXException {
        if (level < RECORD_LEVEL && encounteredFirstRecord) {
            docLevelEventBuffer.endEntity(name);
        } else {
            super.endEntity(name);
        }
    }

    @Override
    public void startCDATA() throws SAXException {
        if (level < RECORD_LEVEL && encounteredFirstRecord) {
            docLevelEventBuffer.startCDATA();
        } else {
            super.startCDATA();
        }
    }

    @Override
    public void startDTD(String name, String publicId, String systemId) throws SAXException {
        if (level < RECORD_LEVEL && encounteredFirstRecord) {
            docLevelEventBuffer.startDTD(name, publicId, systemId);
        } else {
            super.startDTD(name, publicId, systemId);
        }
    }

    @Override
    public void startEntity(String name) throws SAXException {
        if (level < RECORD_LEVEL && encounteredFirstRecord) {
            docLevelEventBuffer.startEntity(name);
        } else {
            super.startEntity(name);
        }
    }

    private static class SaxEventVerifier implements ContentHandler {

        UnboundedContentHandlerBuffer base = new UnboundedContentHandlerBuffer();
        int baseIndex = -1;
        boolean recording = false;
        boolean verifying = false;

        public void recordStart() {
            recording = true;
            verifying = false;
        }

        public void recordEnd() {
            recording = false;
        }

        public void verifyStart() {
            recording = false;
            verifying = true;
            baseIndex = 0;
        }

        public void verifyEnd() {
            if (verifying) {
                verifying = false;
                if (baseIndex < base.size()) {
                    throw new IllegalStateException("required base events not found");
                }
            }
        }

        @Override
        public void startDocument() throws SAXException {
            if (verifying) {
                if (!base.verifyStartDocument(baseIndex++)) {
                    throw new IllegalStateException("base event verification failed");
                }
            } else if (recording) {
                base.startDocument();
            }
        }

        @Override
        public void startPrefixMapping(String prefix, String uri) throws SAXException {
            if (verifying) {
                if (!base.verifyStartPrefixMapping(baseIndex++, prefix, uri)) {
                    throw new IllegalStateException("base event verification failed");
                }
            } else if (recording) {
                base.startPrefixMapping(prefix, uri);
            }
        }

        @Override
        public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
            if (verifying) {
                if (!base.verifyStartElement(baseIndex++, uri, localName, qName, atts)) {
                    throw new IllegalStateException("base event verification failed");
                }
            } else if (recording) {
                base.startElement(uri, localName, qName, atts);
            }
        }

        @Override
        public void endDocument() throws SAXException {
            if (verifying) {
                if (!base.verifyEndDocument(baseIndex++)) {
                    throw new IllegalStateException("base event verification failed");
                }
            } else if (recording) {
                base.endDocument();
            }
        }

        @Override
        public void endPrefixMapping(String prefix) throws SAXException {
            if (verifying) {
                if (!base.verifyEndPrefixMapping(baseIndex++, prefix)) {
                    throw new IllegalStateException("base event verification failed");
                }
            } else if (recording) {
                base.endPrefixMapping(prefix);
            }
        }

        @Override
        public void endElement(String uri, String localName, String qName) throws SAXException {
            if (verifying) {
                if (!base.verifyEndElement(baseIndex++, uri, localName, qName)) {
                    throw new IllegalStateException("base event verification failed");
                }
            } else if (recording) {
                base.endElement(uri, localName, qName);
            }
        }

        @Override
        public void characters(char[] ch, int start, int length) throws SAXException {
        }

        @Override
        public void ignorableWhitespace(char[] ch, int start, int length) throws SAXException {
        }

        @Override
        public void processingInstruction(String target, String data) throws SAXException {
        }

        @Override
        public void skippedEntity(String name) throws SAXException {
        }

        @Override
        public void setDocumentLocator(Locator locator) {
        }
    }
}
