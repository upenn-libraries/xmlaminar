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

package edu.upennlib.ingestor.sax.integrator.complex;

import edu.upennlib.ingestor.sax.xsl.BufferingXMLFilter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.TTCCLayout;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.XMLFilterImpl;

/**
 *
 * @author michael
 */
public class StatefulXMLFilter extends XMLFilterImpl implements Runnable, IdQueryable {

    public static final String INTEGRATOR_URI = "http://integrator";
    public static enum State {WAIT, SKIP, STEP, PLAY}
    private State state = State.STEP;
    private int level = -1;
    private int refLevel = -1;

    private boolean selfId = false;
    private LinkedList<Comparable> id = new LinkedList<Comparable>();

    private InputSource inputSource = new InputSource();

    protected Logger logger = Logger.getLogger(getClass());

    public StatefulXMLFilter() {
        logger.addAppender(new ConsoleAppender(new TTCCLayout(), "System.out"));
        logger.setLevel(Level.TRACE);
    }

    public void setInputSource(InputSource inputSource) {
        this.inputSource = inputSource;
    }

    @Override
    public void run() {
        try {
            parse(inputSource);
        } catch (SAXException ex) {
            throw new RuntimeException(ex);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    private void block() {
        while (state != State.WAIT) {
            synchronized(this) {
                try {
                    wait();
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
            }
        }
    }

    @Override
    public boolean self() {
        block();
        return selfId;
    }

    @Override
    public int getLevel() {
        block();
        return level;
    }

    @Override
    public Comparable getId() {
        block();
        return id.peek();
    }

    private static String attsToString(Attributes atts) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < atts.getLength(); i++) {
            sb.append(atts.getQName(i)).append("=\"").append(atts.getValue(i)).append("\", ");
        }
        return sb.toString();
    }

    @Override
    public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
        level++;
        switch (state) {
            case WAIT:
                throw new IllegalStateException();
            case SKIP:
                break;
            case PLAY:
                super.startElement(uri, localName, qName, atts);
                return;
            case STEP:
                if (!uri.equals(INTEGRATOR_URI)) {
                    throw new IllegalStateException();
                } else if (level > 0) {
                    String idString = atts.getValue("id");
                    if (idString == null) {
                        throw new IllegalStateException("idString=="+idString+" for " + uri + ", " + localName + ", " + qName + ", " + attsToString(atts));
                    } else {
                        Comparable localId = new IdUpenn(localName, idString);
                        logger.trace("id="+localId);
                        id.push(localId);
                    }
                    if ("true".equals(atts.getValue("self"))) {
                        selfId = true;
                    }
                }
                state = State.WAIT;
                while (state == State.WAIT) {
                    synchronized (this) {
                        notify();
                        try {
                            wait();
                        } catch (InterruptedException ex) {
                            throw new RuntimeException(ex);
                        }
                    }
                }
                if (state == State.PLAY || state == State.SKIP) {
                    refLevel = level + 1;
                }
                if (state == State.PLAY && refLevel != level + 1) {
                    super.startElement(uri, localName, qName, atts);
                } else if (state == State.STEP) {
                    startElementBuffer.startElement(uri, localName, qName, atts);
                }
        }
    }
    private final BufferingXMLFilter endElementBuffer = new BufferingXMLFilter();
    private final BufferingXMLFilter startElementBuffer = new BufferingXMLFilter();

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        switch (state) {
            case WAIT:
                throw new IllegalStateException();
            case SKIP:
                if (level == refLevel) {
                    startElementBuffer.clear();
                    endElementBuffer.clear();
                    state = State.STEP;
                }
            case STEP:
                if (!uri.equals(INTEGRATOR_URI)) {
                    throw new IllegalStateException();
                }
                if (!localName.equals("root")) {
                    id.pop();
                }
                if (selfId) {
                    selfId = false;
                    break;
                }
                endElementBuffer.endElement(uri, localName, qName);
            case PLAY:
                if (level == refLevel) {
                    startElementBuffer.clear();
                    endElementBuffer.clear();
                    state = State.STEP;
                }
                super.endElement(uri, localName, qName);
        }
        level--;
    }

    @Override
    public void skipOutput() {
        if (state != State.WAIT) {
            throw new IllegalStateException("expected state WAIT, found: "+state);
        }
        state = State.SKIP;
        synchronized(this) {
            notify();
        }
    }

    @Override
    public void writeOutput(ContentHandler ch) {
        setContentHandler(ch);
        if (state != State.WAIT) {
            throw new IllegalStateException("expected state WAIT, found: "+state);
        }
        state = State.PLAY;
        synchronized(this) {
            notify();
            try {
                wait();
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    @Override
    public void writeStartElements(ContentHandler ch) {
        if (state != State.WAIT) {
            throw new IllegalStateException("expected state WAIT, found: "+state);
        }
        try {
            startElementBuffer.flush(ch);
        } catch (SAXException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void writeEndElements(ContentHandler ch) {
        if (state != State.WAIT) {
            throw new IllegalStateException("expected state WAIT, found: "+state);
        }
        try {
            endElementBuffer.flush(ch);
        } catch (SAXException ex) {
            throw new RuntimeException(ex);
        }
    }

}
