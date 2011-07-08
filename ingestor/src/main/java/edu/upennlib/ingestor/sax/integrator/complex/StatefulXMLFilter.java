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
import edu.upennlib.ingestor.sax.xsl.SaxEventType;
import java.io.EOFException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.Random;
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

    @Override
    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public StatefulXMLFilter() {
        logger.addAppender(new ConsoleAppender(new TTCCLayout(), "System.out"));
        logger.setLevel(Level.TRACE);
        setContentHandler(workingBuffer);
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
                break;
            case STEP:
                if (getContentHandler() != workingBuffer) {
                    throw new IllegalStateException(buffersToString()+"contentHandler: "+getContentHandler());
                }
                pushStartBuffer();
                workingBuffer.flush(innerStartElementBuffer);
                setContentHandler(innerStartElementBuffer);
                super.startElement(uri, localName, qName, atts);
                if (!uri.equals(INTEGRATOR_URI)) {
                    throw new IllegalStateException(name+" expected uri: " + INTEGRATOR_URI + "; found: " + uri + ", " + localName + ", " + qName);
                } else if (level > 0) {
                    String idString = atts.getValue("id");
                    if (idString == null) {
                        throw new IllegalStateException("level="+level+" idString=="+idString+" for " + uri + ", " + localName + ", " + qName + ", " + attsToString(atts));
                    } else {
                        Comparable localId = new IdUpenn(localName, idString);
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
                }
                if (state == State.PLAY && refLevel != level) {
                    super.startElement(uri, localName, qName, atts);
                }
        }
        lastWasStartElement = true;
    }
    private BufferingXMLFilter outerEndElementBuffer = new BufferingXMLFilter();
    private BufferingXMLFilter innerEndElementBuffer = new BufferingXMLFilter();
    private BufferingXMLFilter innerStartElementBuffer = new BufferingXMLFilter();
    private BufferingXMLFilter outerStartElementBuffer = new BufferingXMLFilter();
    private BufferingXMLFilter workingBuffer = new BufferingXMLFilter();

    private Boolean lastWasStartElement = null;

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        level--;
        switch (state) {
            case WAIT:
                throw new IllegalStateException();
            case STEP:
                if (!uri.equals(INTEGRATOR_URI)) {
                    throw new IllegalStateException();
                }
                if (!localName.equals("root")) {
                    try {
                        id.pop();
                    } catch (NoSuchElementException ex) {
                        System.out.println(name + " level=" + level + ", id=null, endElement(" + uri + ", " + localName + ", " + qName + ")");
                        ex.printStackTrace(System.out);
                    }
                }
                if (getContentHandler() != workingBuffer) {
                    throw new IllegalStateException();
                }
                popStartBuffer();
                if (!lastWasStartElement) {
                    workingBuffer.flush(outerEndElementBuffer);
                }
                rotateEndBuffer();
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
                if (level < refLevel) {
                    refLevel = -1;
                    selfId = false;
                    state = State.STEP;
                    if (!localName.equals("root")) {
                        id.pop();
                    }
                    popStartBuffer();
                    workingBuffer.clear();
                    setContentHandler(workingBuffer);
                }
                break;
            case PLAY:
                if (level < refLevel) {
                    refLevel = -1;
                    if (!selfId) {
                        throw new IllegalStateException();
                    } else {
                        selfId = false;
                    }
                    if (!localName.equals("root")) {
                        id.pop();
                    }
                    popStartBuffer();
                    workingBuffer.clear();
                    setContentHandler(workingBuffer);
                    state = State.STEP;
                }
                super.endElement(uri, localName, qName);
        }
        lastWasStartElement = false;
    }

    private boolean finished = false;

    private void pushStartBuffer() {
        BufferingXMLFilter tmp = outerStartElementBuffer;
        outerStartElementBuffer = innerStartElementBuffer;
        innerStartElementBuffer = tmp;
        innerStartElementBuffer.clear();
    }

    private void popStartBuffer() {
        BufferingXMLFilter tmp = innerStartElementBuffer;
        innerStartElementBuffer = outerStartElementBuffer;
        outerStartElementBuffer = tmp;
        outerStartElementBuffer.clear();
    }

    private void rotateEndBuffer() {
        BufferingXMLFilter tmp = innerEndElementBuffer;
        innerEndElementBuffer = outerEndElementBuffer;
        outerEndElementBuffer = tmp;
        outerEndElementBuffer.clear();
    }

    @Override
    public void endDocument() throws SAXException {
        if (getContentHandler() != workingBuffer) {
            throw new IllegalStateException();
        }
        workingBuffer.flush(outerEndElementBuffer);
        setContentHandler(outerEndElementBuffer);
        super.endDocument();
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
        System.out.println("called skip on "+name+", level="+level+", id="+id.peek());
        if (level == 0 || id == null) {
            throw new IllegalStateException();
        }
        workingBuffer.clear();
        setContentHandler(workingBuffer);
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
        if (selfId) {
            throw new IllegalStateException();
        }
        state = State.STEP;
        workingBuffer.clear();
        setContentHandler(workingBuffer);
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
