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
import edu.upennlib.ingestor.sax.xsl.SaxEventExecutor;
import java.io.EOFException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Iterator;
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
import org.xml.sax.helpers.AttributesImpl;
import org.xml.sax.helpers.XMLFilterImpl;

/**
 *
 * @author michael
 */
public class StatefulXMLFilter extends XMLFilterImpl implements IdQueryable {

    public static final String INTEGRATOR_URI = "http://integrator";
    public static enum State {WAIT, SKIP, STEP, PLAY}
    private State state = State.STEP;
    private int level = -1;
    private int refLevel = -1;

    private boolean selfId = false;
    private LinkedList<Comparable> id = new LinkedList<Comparable>();

    private InputSource inputSource = new InputSource();

    protected Logger logger = Logger.getLogger(getClass());

    private String name;

    public void setName(String name) {
        this.name = name;
    }

    public StatefulXMLFilter() {
        logger.addAppender(new ConsoleAppender(new TTCCLayout(), "System.out"));
        logger.setLevel(Level.TRACE);
        setContentHandler(innerStartElementBuffer);
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
        synchronized (this) {
            while (state != State.WAIT) {
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
        if (isFinished()) {
            throw new IllegalStateException();
        } else {
            block();
            return selfId;
        }
    }

    @Override
    public int getLevel() throws EOFException {
        if (isFinished()) {
            throw new EOFException();
        } else {
            block();
            return level;
        }
    }

    @Override
    public Comparable getId() throws EOFException {
        if (isFinished()) {
            throw new EOFException();
        } else {
            block();
            return id.peek();
        }
    }

    private static String attsToString(Attributes atts) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < atts.getLength(); i++) {
            sb.append(atts.getQName(i)).append("=\"").append(atts.getValue(i)).append("\", ");
        }
        return sb.toString();
    }

    private String buffersToString() {
        StringBuilder sb = new StringBuilder();
        sb.append("outerStartElementBuffer: ").append(outerStartElementBuffer).append("\n");
        sb.append("innerStartElementBuffer: ").append(innerStartElementBuffer).append("\n");
        sb.append("innerEndElementBuffer: ").append(innerEndElementBuffer).append("\n");
        sb.append("outerEndElementBuffer: ").append(outerEndElementBuffer).append("\n");
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
                if (getContentHandler() != innerStartElementBuffer) {
                    if (getContentHandler() == innerEndElementBuffer) {
                        setContentHandler(innerStartElementBuffer);
                    } else {
                        throw new IllegalStateException(buffersToString()+"contentHandler: "+getContentHandler());
                    }
                }
                super.startElement(uri, localName, qName, atts);
                if (!uri.equals(INTEGRATOR_URI)) {
                    throw new IllegalStateException("expected uri: " + INTEGRATOR_URI + "; found: " + uri + ", " + localName + ", " + qName);
                } else if (level > 0) {
                    String idString = atts.getValue("id");
                    if (idString == null) {
                        throw new IllegalStateException("level="+level+" idString=="+idString+" for " + uri + ", " + localName + ", " + qName + ", " + attsToString(atts));
                    } else {
                        Comparable localId = new IdUpenn(localName, idString);
                        //logger.trace("id="+localId);
                        id.push(localId);
                    }
                    if ("true".equals(atts.getValue("self"))) {
                        selfId = true;
                    }
                }
                state = State.WAIT;
                synchronized (this) {
                    while (state == State.WAIT) {
                        notify();
                        try {
                            wait();
                        } catch (InterruptedException ex) {
                            throw new RuntimeException(ex);
                        }
                    }
                }
                if (state == State.PLAY || state == State.SKIP) {
                    refLevel = level;
                    outerStartElementBuffer.clear();
                    innerStartElementBuffer.clear();
                }
                if (state == State.PLAY && refLevel != level) {
                    super.startElement(uri, localName, qName, atts);
                }
        }
    }
    private BufferingXMLFilter outerEndElementBuffer = new BufferingXMLFilter();
    private BufferingXMLFilter innerEndElementBuffer = new BufferingXMLFilter();
    private BufferingXMLFilter innerStartElementBuffer = new BufferingXMLFilter();
    private BufferingXMLFilter outerStartElementBuffer = new BufferingXMLFilter();

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        switch (state) {
            case WAIT:
                throw new IllegalStateException();
            case STEP:
                if (!uri.equals(INTEGRATOR_URI)) {
                    throw new IllegalStateException();
                }
                if (!localName.equals("root")) {
                    id.pop();
                }
                if (getContentHandler() == outerEndElementBuffer) {
                    BufferingXMLFilter tmp = innerEndElementBuffer;
                    innerEndElementBuffer = outerEndElementBuffer;
                    outerEndElementBuffer = tmp;
                    outerEndElementBuffer.clear();
                } else if (getContentHandler() == innerStartElementBuffer) {
                    innerEndElementBuffer.clear();
                    innerStartElementBuffer.flush(outerEndElementBuffer);
                }
                setContentHandler(outerEndElementBuffer);
                super.endElement(uri, localName, qName);
                state = State.WAIT;
                synchronized (this) {
                    while (state == State.WAIT) {
                        notify();
                        try {
                            wait();
                        } catch (InterruptedException ex) {
                            throw new RuntimeException(ex);
                        }
                    }
                }
                break;
            case SKIP:
            case PLAY:
                if (level == refLevel) {
                    if (!selfId) {
                        throw new IllegalStateException();
                    } else {
                        selfId = false;
                    }
                    if (!localName.equals("root")) {
                        id.pop();
                    }
                    innerEndElementBuffer.clear();
                    outerEndElementBuffer.clear();
                    setContentHandler(innerEndElementBuffer);
                    state = State.STEP;
                }
                super.endElement(uri, localName, qName);
        }
        level--;
    }

    private boolean finished = false;

    @Override
    public void endDocument() throws SAXException {
        if (getContentHandler() == innerStartElementBuffer) {
            BufferingXMLFilter tmp = innerEndElementBuffer;
            innerEndElementBuffer = outerEndElementBuffer;
            outerEndElementBuffer = tmp;
            outerEndElementBuffer.clear();
            innerStartElementBuffer.flush(outerEndElementBuffer);
        }
        setContentHandler(outerEndElementBuffer);
        super.endDocument();
        System.out.println("buffering endDocument on sxf."+name);
        synchronized(this) {
            finished = true;
            notify();
        }
    }

    @Override
    public boolean isFinished() {
        return finished;
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
                if (!isFinished()) {
                    wait();
                }
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    SaxEventExecutor see = new SaxEventExecutor();

    @Override
    public void writeOuterStartElement(ContentHandler ch, boolean asSelf) {
        if (state != State.WAIT) {
            throw new IllegalStateException("expected state WAIT, found: "+state);
        }
        try {
            Iterator iter = outerStartElementBuffer.iterator();
            Object[] current = null;
            boolean hasNext = iter.hasNext();
            while (hasNext) {
                current = (Object[]) iter.next();
                if (!(hasNext = iter.hasNext()) && asSelf && self()) {
                    AttributesImpl selfAtts;
                    try {
                        if (current[4] instanceof AttributesImpl) {
                            selfAtts = (AttributesImpl) current[4];
                        } else {
                            selfAtts = new AttributesImpl((Attributes) current[4]);
                        }
                    } catch (ArrayIndexOutOfBoundsException ex) {
                        outerStartElementBuffer.play(null);
                        throw new RuntimeException(""+Arrays.asList(current)+ex);
                    }
                    selfAtts.addAttribute("", "self", "self", "CDATA", "true");
                    current[4] = selfAtts;
                }
                see.executeSaxEvent(ch, current, true, true);
            }
        } catch (SAXException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void writeInnerStartElement(ContentHandler ch) {
        if (state != State.WAIT) {
            throw new IllegalStateException("expected state WAIT, found: "+state);
        }
        try {
            innerStartElementBuffer.flush(ch);
        } catch (SAXException ex) {
            throw new RuntimeException(ex);
        }
    }
    
    @Override
    public void writeInnerEndElement(ContentHandler ch) {
        if (state != State.WAIT && !isFinished()) {
            throw new IllegalStateException("expected state WAIT, found: "+state);
        }
        try {
            innerEndElementBuffer.flush(ch);
        } catch (SAXException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void writeOuterEndElement(ContentHandler ch) {
        if (state != State.WAIT && !isFinished()) {
            throw new IllegalStateException("expected state WAIT, found: "+state);
        }
        try {
            outerEndElementBuffer.flush(ch);
        } catch (SAXException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void step() {
        if (state != State.WAIT) {
            throw new IllegalStateException("expected state WAIT, found: " + state);
        }
        state = State.STEP;
        BufferingXMLFilter tmp = outerStartElementBuffer;
        outerStartElementBuffer = innerStartElementBuffer;
        innerStartElementBuffer = tmp;
        innerStartElementBuffer.clear();
        setContentHandler(innerStartElementBuffer);
        synchronized (this) {
            notify();
            try {
                wait();
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

}
