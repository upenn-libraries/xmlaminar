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

package edu.upenn.library.xmlaminar;

import java.util.ArrayDeque;
import java.util.Deque;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.stream.StreamResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;
import org.xml.sax.XMLFilter;
import org.xml.sax.helpers.XMLFilterImpl;

/**
 *
 * @author magibney
 */
public class BufferingXMLFilter extends XMLFilterImpl {
    
    private static final Logger LOG = LoggerFactory.getLogger(BufferingXMLFilter.class);
    private UnboundedContentHandlerBuffer buf;
    private static enum State { DEFAULT, START_BUFFER, BUFFER, WRITE, DISCARD }
    private State state = State.DEFAULT;
    private int bufferDepth = -1;
    private final Deque<StackEntry> stateStack = new ArrayDeque<StackEntry>();
    private final ContentHandler direct = new DirectLocalXMLFilter();
    private static final boolean DEFAULT_BYPASS_FLUSH = true;
    private static final boolean DEFAULT_BYPASS_DISCARD = false;
    private boolean defaultBypassFlush = DEFAULT_BYPASS_FLUSH;
    private boolean defaultBypassDiscard = DEFAULT_BYPASS_DISCARD;
    
    public static void main(String[] args) throws Exception {
        Transformer t = TransformerFactory.newInstance().newTransformer();
        SAXParserFactory spf = SAXParserFactory.newInstance();
        spf.setNamespaceAware(true);
        XMLFilter blah = new Blah();
        blah.setParent(spf.newSAXParser().getXMLReader());
        t.transform(new SAXSource(blah, new InputSource("./sample.xml")), new StreamResult(System.out));
    }
    
    public static class Blah extends BufferingXMLFilter {
        
        private boolean inRecord = false;
        private boolean nextStart = false;
        
        @Override
        public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
            if (nextStart) {
                nextStart = false;
                System.out.println("nextStart: "+qName);
            }
            if ("marc:record".equals(qName)) {
                startBuffer();
                inRecord = true;
                nextStart = true;
                System.out.println("here");
            }
            super.startElement(uri, localName, qName, atts);
        }

        @Override
        public void characters(char[] ch, int start, int length) throws SAXException {
            if (inRecord) {
                inRecord = false;
                System.out.println("flushing: "+new String(ch, start, length));
            }
            super.characters(ch, start, length); //To change body of generated methods, choose Tools | Templates.
        }
        
    }
    
    private static class StackEntry {
        private final int bufferDepth;
        private final State state;
        private final UnboundedContentHandlerBuffer buf;
        private StackEntry(int bufferDepth, State state, UnboundedContentHandlerBuffer buf) {
            this.bufferDepth = bufferDepth;
            this.state = state;
            this.buf = buf;
        }
    }
    
    public BufferingXMLFilter() {
        buf = new UnboundedContentHandlerBuffer();
    }
    
    public BufferingXMLFilter(int initialBufferSize) {
        buf = new UnboundedContentHandlerBuffer(initialBufferSize);
    }
    
    public UnboundedContentHandlerBuffer getBuffer() {
        // for now just return a new one; may want to reuse or set initial size conditionally
        return new UnboundedContentHandlerBuffer();
    }
    
    protected void startBuffer() {
        switch (state) {
            case START_BUFFER:
            case BUFFER:
                stateStack.push(new StackEntry(bufferDepth, state, buf));
                bufferDepth = 0;
                buf = getBuffer();
                break;
            case WRITE:
                stateStack.push(new StackEntry(bufferDepth, state, null));
                bufferDepth = 0;
                break;
            case DEFAULT:
                bufferDepth = 0;
                break;
            case DISCARD:
                throw new IllegalStateException("called startBuffer() while in state "+State.DISCARD);
        }
        super.setContentHandler(buf);
        state = State.BUFFER;
    }

    private ContentHandler externalContentHandler = null;
    
    @Override
    public ContentHandler getContentHandler() {
        return externalContentHandler;
    }

    @Override
    public void setContentHandler(ContentHandler handler) {
        externalContentHandler = handler;
        if (super.getContentHandler() != buf && super.getContentHandler() != devNullContentHandler) {
            super.setContentHandler(handler);
        }
    }

    public boolean isDefaultBypassFlush() {
        return defaultBypassFlush;
    }

    public void setDefaultBypassFlush(boolean bypassFlush) {
        this.defaultBypassFlush = bypassFlush;
    }

    public boolean isDefaultBypassDiscard() {
        return defaultBypassDiscard;
    }

    public void setDefaultBypassDiscard(boolean bypassDiscard) {
        this.defaultBypassDiscard = bypassDiscard;
    }
    
    protected void flush(boolean bypass) throws SAXException {
        this.bypass = bypass;
        switch (state) {
            case START_BUFFER:
            case BUFFER:
                state = State.WRITE;
                break;
            case WRITE:
                LOG.warn("called flush() while in state {}", State.WRITE);
                break;
            case DEFAULT:
                LOG.warn("called flush() while in state {}", State.DEFAULT);
                break;
            case DISCARD:
                throw new IllegalStateException("called flush() while in state "+State.DISCARD);
        }
        System.out.println("flushing "+buf.size());
        buf.flush(externalContentHandler, null);
        super.setContentHandler(externalContentHandler);
        if (bypass) {
            if (getParent().getContentHandler() != this) {
                throw new IllegalStateException("violates assumption");
            }
            getParent().setContentHandler(direct);
        }
        state = State.WRITE;
    }
    
    protected void discard(boolean bypass) {
        this.bypass = bypass;
        switch (state) {
            case START_BUFFER:
            case BUFFER:
                state = State.DISCARD;
                break;
            case DISCARD:
            case DEFAULT:
                LOG.warn("called discard() while in state {}", state);
                return;
            case WRITE:
                throw new IllegalStateException("called discard() while in state "+State.WRITE);
        }
        buf.clear();
        super.setContentHandler(devNullContentHandler);
        if (bypass) {
            if (getParent().getContentHandler() != this) {
                throw new IllegalStateException("violates assumption");
            }
            getParent().setContentHandler(direct);
        }
        state = State.DISCARD;
    }

    private static final ContentHandler devNullContentHandler = new DevNullContentHandler();
    
    private boolean bypass = false;
    
    private boolean defaultDiscard = false;
    
    public boolean isDefaultDiscard() {
        return defaultDiscard;
    }
    
    public void setDefaultDiscard(boolean defaultDiscard) {
        this.defaultDiscard = defaultDiscard;
    }
    
    private void endEvent() throws SAXException {
        if (--bufferDepth < 0) {
            switch (state) {
                case START_BUFFER:
                case BUFFER:
                    System.out.println("state="+state);
                    if (defaultDiscard) {
                        discard(defaultBypassDiscard);
                    } else {
                        flush(defaultBypassFlush);
                    }
                    break;
            }
            switch (state) {
                case WRITE:
                case DISCARD:
                    if (bypass) {
                        getParent().setContentHandler(this);
                    }
                    break;
            }
            if (!stateStack.isEmpty()) {
                StackEntry outer = stateStack.pop();
                state = outer.state;
                bufferDepth = outer.bufferDepth;
                bypass = false; // must be in order to have had an inner stackEntry
                if (outer.buf != null) {
                    buf = outer.buf;
                }
                switch (state) {
                    case BUFFER:
                        super.setContentHandler(buf);
                        break;
                    case WRITE:
                        super.setContentHandler(externalContentHandler);
                        break;
                    default:
                        throw new AssertionError("illegal stack element state: "+state);
                }
            } else {
                super.setContentHandler(externalContentHandler);
                state = State.DEFAULT;
            }
        }
    }

    
    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        localEndElement(uri, localName, qName);
    }
    
    private void localEndElement(String uri, String localName, String qName) throws SAXException {
        switch (state) {
            case START_BUFFER:
            case BUFFER:
            case WRITE:
            case DISCARD:
                endEvent();
        }
        super.endElement(uri, localName, qName);
    }

    
    @Override
    public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
        localStartElement(uri, localName, qName, atts);
    }
    
    private void localStartElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
        switch (state) {
            case START_BUFFER:
                state = State.BUFFER;
                if (buf.size() == 0) {
                    break;
                }
            case BUFFER:
            case WRITE:
            case DISCARD:
                bufferDepth++;
        }
        super.startElement(uri, localName, qName, atts);
    }

    @Override
    public void endPrefixMapping(String prefix) throws SAXException {
        localEndPrefixMapping(prefix);
    }

    private void localEndPrefixMapping(String prefix) throws SAXException {
        switch (state) {
            case START_BUFFER:
            case BUFFER:
            case WRITE:
            case DISCARD:
                endEvent();
        }
        super.endPrefixMapping(prefix);
    }

    @Override
    public void startPrefixMapping(String prefix, String uri) throws SAXException {
        localStartPrefixMapping(prefix, uri);
    }

    private void localStartPrefixMapping(String prefix, String uri) throws SAXException {
        switch (state) {
            case START_BUFFER:
                state = State.BUFFER;
                if (buf.size() == 0) {
                    break;
                }
            case BUFFER:
            case WRITE:
                bufferDepth++;
        }
        super.startPrefixMapping(prefix, uri);
    }
    
    private class DirectLocalXMLFilter implements ContentHandler {

        @Override
        public void setDocumentLocator(Locator locator) {
            BufferingXMLFilter.super.setDocumentLocator(locator);
        }

        @Override
        public void startDocument() throws SAXException {
            BufferingXMLFilter.super.startDocument();
        }

        @Override
        public void endDocument() throws SAXException {
            BufferingXMLFilter.super.endDocument();
        }

        @Override
        public void startPrefixMapping(String prefix, String uri) throws SAXException {
            localStartPrefixMapping(prefix, uri);
        }

        @Override
        public void endPrefixMapping(String prefix) throws SAXException {
            localEndPrefixMapping(prefix);
        }

        @Override
        public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
            localStartElement(uri, localName, qName, atts);
        }

        @Override
        public void endElement(String uri, String localName, String qName) throws SAXException {
            localEndElement(uri, localName, qName);
        }

        @Override
        public void characters(char[] ch, int start, int length) throws SAXException {
            BufferingXMLFilter.super.characters(ch, start, length);
        }

        @Override
        public void ignorableWhitespace(char[] ch, int start, int length) throws SAXException {
            BufferingXMLFilter.super.ignorableWhitespace(ch, start, length);
        }

        @Override
        public void processingInstruction(String target, String data) throws SAXException {
            BufferingXMLFilter.super.processingInstruction(target, data);
        }

        @Override
        public void skippedEntity(String name) throws SAXException {
            BufferingXMLFilter.super.skippedEntity(name);
        }
        
    }
    
}
