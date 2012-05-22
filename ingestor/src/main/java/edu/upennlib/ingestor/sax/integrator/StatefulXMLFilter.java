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

package edu.upennlib.ingestor.sax.integrator;

import edu.upennlib.xmlutils.SAXFeatures;
import edu.upennlib.xmlutils.UnboundedContentHandlerBuffer;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.PrintStream;
import java.util.LinkedList;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.XMLFilterImpl;

/**
 *
 * @author michael
 */
public class StatefulXMLFilter extends XMLFilterImpl implements IdQueryable {


    private final boolean debugging = true;
    public static final String INTEGRATOR_URI = "http://integrator";
    public static enum State {WAIT, SKIP, STEP, PLAY}
    private State state = State.STEP;
    private int level = -1;
    private int refLevel = -1;

    private boolean selfId = false;
    private LinkedList<Comparable> id = new LinkedList<Comparable>();

    private InputSource inputSource = new InputSource();

    private static final Logger logger = Logger.getLogger(StatefulXMLFilter.class);

    private String name;
    private final Lock lock = new ReentrantLock();
    private final Condition stateNotWait = lock.newCondition();
    private final Condition stateIsWait = lock.newCondition();

    @Override
    public void setName(String name) {
        this.name = name;
    }

    private boolean stringIntern;

    @Override
    public void parse(InputSource input) throws SAXException, IOException {
        stringIntern = getFeature(SAXFeatures.STRING_INTERNING);
        super.parse(input);
    }

    @Override
    public void parse(String systemId) throws SAXException, IOException {
        stringIntern = getFeature(SAXFeatures.STRING_INTERNING);
        super.parse(systemId);
    }

    @Override
    public String getName() {
        return name;
    }

    public StatefulXMLFilter() {
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
        try {
            lock.lock();
            while (state != State.WAIT) {
                try {
                    stateIsWait.await();
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean self() {
        if (debugging && isFinished()) {
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
        int length = atts.getLength();
        if (length <= 0) {
            return "[]";
        } else {
            StringBuilder sb = new StringBuilder();
            sb.append(atts.getQName(0)).append("=\"").append(atts.getValue(0));
            for (int i = 1; i < length; i++) {
                sb.append("\", ").append(atts.getQName(i)).append("=\"").append(atts.getValue(i));
            }
            return sb.toString();
        }
    }

    public String buffersToString() {
        StringBuilder sb = new StringBuilder();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);
        sb.append("\touterStartElementBuffer: ").append(bToS(outerStartElementBuffer, ps, baos));
        sb.append("\tinnerStartElementBuffer: ").append(bToS(innerStartElementBuffer, ps, baos));
        sb.append("\tinnerEndElementBuffer: ").append(bToS(innerEndElementBuffer, ps, baos));
        sb.append("\touterEndElementBuffer: ").append(bToS(outerEndElementBuffer, ps, baos));
        return sb.toString();
    }

    private static String bToS(UnboundedContentHandlerBuffer buffer, PrintStream ps, ByteArrayOutputStream baos) {
        ps.flush();
        baos.reset();
        try {
            buffer.dump(ps, false);
        } catch (SAXException ex) {
            throw new RuntimeException(ex);
        }
        ps.flush();
        if (baos.size() == 0) {
            return "\n";
        } else {
            return baos.toString();
        }
    }

    private int selfLevel = -1;

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
                if (debugging && getContentHandler() != workingBuffer) {
                    throw new IllegalStateException(buffersToString()+"contentHandler: "+getContentHandler());
                }
                pushStartBuffer();
                workingBuffer.flush(innerStartElementBuffer, innerStartElementBuffer);
                innerStartElementBuffer.startElement(uri, localName, qName, atts);
                if (debugging) {
                    if (stringIntern) {
                        if (uri != INTEGRATOR_URI) {
                            throw new IllegalStateException(name + " expected uri: " + INTEGRATOR_URI + "; found: " + uri + ", " + localName + ", " + qName);
                        }
                    } else {
                        if (!uri.equals(INTEGRATOR_URI)) {
                            throw new IllegalStateException(name + " expected uri: " + INTEGRATOR_URI + "; found: " + uri + ", " + localName + ", " + qName);
                        }
                    }
                }
                if (level > 0) {
                    Comparable localId = new IdUpenn(localName, atts.getValue("id"), stringIntern);
                    id.push(localId);
                    if (level == selfLevel) {
                        selfId = true;
                        if (debugging && !"true".equals(atts.getValue("self"))) {
                            throw new IllegalArgumentException();
                        }
                    } else if (selfLevel == -1) {
                        if ("true".equals(atts.getValue("self"))) {
                            selfLevel = level;
                            selfId = true;
                        }
                    }
                }
                state = State.WAIT;
                try {
                    lock.lock();
                    stateIsWait.signal();
                    while (state == State.WAIT) {
                        try {
                            stateNotWait.await();
                        } catch (InterruptedException ex) {
                            throw new RuntimeException(ex);
                        }
                    }
                } finally {
                    lock.unlock();
                }
        }
        lastWasStartElement = true;
    }
    private UnboundedContentHandlerBuffer outerEndElementBuffer = new UnboundedContentHandlerBuffer();
    private UnboundedContentHandlerBuffer innerEndElementBuffer = new UnboundedContentHandlerBuffer();
    private UnboundedContentHandlerBuffer innerStartElementBuffer = new UnboundedContentHandlerBuffer();
    private UnboundedContentHandlerBuffer outerStartElementBuffer = new UnboundedContentHandlerBuffer();
    private final UnboundedContentHandlerBuffer workingBuffer = new UnboundedContentHandlerBuffer();

    private boolean lastWasStartElement = true;

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        switch (state) {
            case WAIT:
                throw new IllegalStateException();
            case STEP:
                if (debugging) {
                    if (stringIntern) {
                        if (uri != INTEGRATOR_URI) {
                            throw new IllegalStateException();
                        }
                    } else {
                        if (!uri.equals(INTEGRATOR_URI)) {
                            throw new IllegalStateException();
                        }
                    }
                }
                if (level > 0) {
                    id.pop();
                }
                if (debugging && getContentHandler() != workingBuffer) {
                    throw new IllegalStateException();
                }
                //if (!lastWasStartElement) {
                    rotateEndBuffer();
                //}
                workingBuffer.flush(outerEndElementBuffer, outerEndElementBuffer);
                outerEndElementBuffer.endElement(uri, localName, qName);
                state = State.WAIT;
                try {
                    lock.lock();
                    stateIsWait.signal();
                    while (state == State.WAIT) {
                        try {
                            stateNotWait.await();
                        } catch (InterruptedException ex) {
                            throw new RuntimeException(ex);
                        }
                    }
                } finally {
                    lock.unlock();
                }
                break;
            case SKIP:
                if (level <= refLevel) {
                    refLevel = -1;
                    selfId = false;
                    state = State.STEP;
                    if (level > 0) {
                        id.pop();
                    }
                    workingBuffer.clear();
                    setContentHandler(workingBuffer);
                }
                break;
            case PLAY:
                if (level > refLevel) {
                    super.endElement(uri, localName, qName);
                } else {
                    refLevel = -1;
                    if (!selfId) {
                        throw new IllegalStateException();
                    } else {
                        selfId = false;
                    }
                    if (level > 0) {
                        id.pop();
                    }
                    workingBuffer.clear();
                    setContentHandler(workingBuffer);
                    state = State.STEP;
                    innerEndElementBuffer.clear();
                    outerEndElementBuffer.clear();
                    outerEndElementBuffer.endElement(uri, localName, qName);
                }
        }
        if (level > 0) {
            popStartBuffer();
        }
        level--;
        lastWasStartElement = false;
    }

    private boolean finished = false;

    /**
     * Moves inner buffer to outer, clears inner.
     */
    private void pushStartBuffer() {
        UnboundedContentHandlerBuffer tmp = outerStartElementBuffer;
        outerStartElementBuffer = innerStartElementBuffer;
        innerStartElementBuffer = tmp;
        innerStartElementBuffer.clear();
    }

    /**
     * Moves outer to inner, clears outer.
     */
    private void popStartBuffer() {
        UnboundedContentHandlerBuffer tmp = innerStartElementBuffer;
        innerStartElementBuffer = outerStartElementBuffer;
        outerStartElementBuffer = tmp;
        outerStartElementBuffer.clear();
    }

    /**
     * Moves outer to inner, clears outer.
     */
    private void rotateEndBuffer() {
        UnboundedContentHandlerBuffer tmp = innerEndElementBuffer;
        innerEndElementBuffer = outerEndElementBuffer;
        outerEndElementBuffer = tmp;
        outerEndElementBuffer.clear();
    }

    @Override
    public void endDocument() throws SAXException {
        if (getContentHandler() != workingBuffer) {
            throw new IllegalStateException();
        }
        workingBuffer.flush(outerEndElementBuffer, outerEndElementBuffer);
        outerEndElementBuffer.endDocument();
        try {
            lock.lock();
            finished = true;
            state = State.WAIT;
            stateIsWait.signal();
        } finally {
            lock.unlock();
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
        if (level == 0 || id == null) {
            throw new IllegalStateException();
        }
        workingBuffer.clear();
        setContentHandler(workingBuffer);
        refLevel = level;
        state = State.SKIP;
        try {
            lock.lock();
            stateNotWait.signal();
        } finally {
            lock.unlock();
        }
    }

    public static void main(String[] args) throws ParserConfigurationException, SAXException, IOException {
        BasicConfigurator.configure();
        logger.setLevel(Level.TRACE);
        Logger.getRootLogger().setLevel(Level.TRACE);
        SAXParserFactory spf = SAXParserFactory.newInstance();
        spf.setNamespaceAware(true);
        StatefulXMLFilter sxf = new StatefulXMLFilter();
        sxf.setParent(spf.newSAXParser().getXMLReader());
        Thread t = new ParseThread(sxf);
        t.start();
        ContentHandler out = new EchoingContentHandler();
        while (!sxf.isFinished()) {
            if (!sxf.self()) {
                do {
                    sxf.step();
                    System.out.print(sxf.buffersToString());
                } while (!sxf.isFinished() && !sxf.self());
                if (!sxf.isFinished()) {
                    sxf.writeOutput(out);
                }
            } else {
                System.out.print(sxf.buffersToString());
                sxf.writeOutput(out);
            }
        }
    }

    private static class ParseThread extends Thread {

        private final StatefulXMLFilter sxf;

        private ParseThread(StatefulXMLFilter sxf) {
            this.sxf = sxf;
        }

        @Override
        public void run() {
            try {
                sxf.parse("./src/test/resources/hldg.xml");
            } catch (SAXException ex) {
                throw new RuntimeException(ex);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }

    }

    @Override
    public void writeOutput(ContentHandler ch) {
        if (state != State.WAIT) {
            throw new IllegalStateException("expected state WAIT, found: "+state);
        }
        setContentHandler(ch);
        refLevel = level;
        state = State.PLAY;
        try {
            lock.lock();
            stateNotWait.signal();
            if (!isFinished()) {
                try {
                    stateIsWait.await();
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void writeOuterStartElement(ContentHandler ch, boolean asSelf) {
        try {
            outerStartElementBuffer.writeWithFinalElementSelfAttribute(ch, null, asSelf);
        } catch (SAXException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void writeInnerStartElement(ContentHandler ch) {
        if (debugging && state != State.WAIT) {
            throw new IllegalStateException("expected state WAIT, found: "+state);
        }
        try {
            innerStartElementBuffer.flush(ch, null);
        } catch (SAXException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void writeInnerEndElement(ContentHandler ch) {
        if (debugging && state != State.WAIT && !isFinished()) {
            throw new IllegalStateException("expected state WAIT, found: "+state);
        }
        try {
            innerEndElementBuffer.flush(ch, null);
        } catch (SAXException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void writeOuterEndElement(ContentHandler ch) {
        if (debugging && state != State.WAIT && !isFinished()) {
            throw new IllegalStateException("expected state WAIT, found: "+state);
        }
        try {
            outerEndElementBuffer.flush(ch, null);
        } catch (SAXException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void step() {
        if (debugging && state != State.WAIT) {
            throw new IllegalStateException("expected state WAIT, found: " + state);
        }
        if (debugging && selfId) {
            throw new IllegalStateException();
        }
        state = State.STEP;
        workingBuffer.clear();
        setContentHandler(workingBuffer);
        try {
            lock.lock();
            stateNotWait.signal();
            try {
                stateIsWait.await();
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            }
        } finally {
            lock.unlock();
        }
    }

}
