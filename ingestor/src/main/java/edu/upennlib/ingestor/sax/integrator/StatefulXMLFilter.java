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

import edu.upennlib.paralleltransformer.InputSourceXMLReader;
import edu.upennlib.xmlutils.EchoingContentHandler;
import edu.upennlib.xmlutils.DevNullContentHandler;
import edu.upennlib.xmlutils.SAXFeatures;
import edu.upennlib.xmlutils.UnboundedContentHandlerBuffer;
import edu.upennlib.xmlutils.VolatileXMLFilterImpl;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLFilterImpl;

/**
 *
 * @author michael
 */
public class StatefulXMLFilter extends VolatileXMLFilterImpl implements IdQueryable {


    private static final boolean debugging = true;
    public static final String INTEGRATOR_URI = "http://integrator";
    public static enum State {WAIT, SKIP, STEP, PLAY}
    private volatile State state = State.STEP;
    private volatile int level = -1;
    private volatile int refLevel = -1;

    private volatile boolean selfId = false;
    private Deque<Comparable> id = new ArrayDeque<Comparable>();

    private InputSource inputSource = new InputSource();

    private static final Logger logger = Logger.getLogger(StatefulXMLFilter.class);

    private String name;
    private final Lock lock = new ReentrantLock();
    private final Condition stateNotWait = lock.newCondition();
    private final Condition stateIsWait = lock.newCondition();

    @Override
    public void reset() {
        state = State.STEP;
        level = -1;
        selfLevel = -1;
        refLevel = -1;
        finished = false;
        lastWasEndElement = false;
        writingOutput = false;
        tmpBuffer.clear();
        clearBuffers(endElementEventStack);
        clearBuffers(startElementEventStack);
    }
    
    private void clearBuffers(UnboundedContentHandlerBuffer[] buffers) {
        for (UnboundedContentHandlerBuffer uchb : buffers) {
            uchb.clear();
        }
    }

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

    public StatefulXMLFilter(int depth) {
        startElementEventStack = new UnboundedContentHandlerBuffer[depth];
        endElementEventStack = new UnboundedContentHandlerBuffer[depth];
        for (int i = 0; i < depth; i++) {
            startElementEventStack[i] = new UnboundedContentHandlerBuffer();
            endElementEventStack[i] = new UnboundedContentHandlerBuffer();
        }
        setContentHandler(tmpBuffer);
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

    @Override
    public boolean self() {
        if (isFinished()) {
            throw new IllegalStateException();
        } else {
            return selfId;
        }
    }

    @Override
    public int getLevel() throws EOFException {
        if (isFinished()) {
            throw new EOFException();
        } else {
            return level;
        }
    }

    @Override
    public Comparable getId() throws EOFException {
        if (isFinished()) {
            throw new EOFException();
        } else {
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

    public String buffersToString(int depth) {
        StringBuilder sb = new StringBuilder();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);
        sb.append("StartBuffer: \n");
        for (int i = 0; i <= level; i++) {
            sb.append('\t').append(i).append(": ").append(bToS(startElementEventStack[i], ps, baos));
        }
        sb.append("EndBuffer: \n");
        for (int i = depth - 1; i >= 0; i--) {
            sb.append('\t').append(i).append(": ").append(bToS(endElementEventStack[i], ps, baos));
        }
        sb.append("tmpBuffer: ").append(bToS(tmpBuffer, ps, baos));
        return sb.toString();
    }

    @Override
    public String buffersToString() {
        return buffersToString(startElementEventStack.length);
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
                throw new IllegalStateException("at level "+level);
            case SKIP:
                break;
            case PLAY:
                super.startElement(uri, localName, qName, atts);
                break;
            case STEP:
                push();
                startElementEventStack[level].startElement(uri, localName, qName, atts);
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
                            throw new IllegalArgumentException(Thread.currentThread()+" expected self at level "+level+", found "+id);
                        }
                    } else if (selfLevel == -1) {
                        if ("true".equals(atts.getValue("self"))) {
                            selfLevel = level;
                            selfId = true;
                        }
                    }
                }
                writingOutput = false;
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
        lastWasEndElement = false;
    }
    
    private boolean lastWasEndElement = false;

    private UnboundedContentHandlerBuffer tmpBuffer = new UnboundedContentHandlerBuffer();

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
                pop();
                super.endElement(uri, localName, qName);
                break;
            case SKIP:
                if (level <= refLevel) {
                    refLevel = -1;
                    selfId = false;
                    state = State.STEP;
                    if (level > 0) {
                        id.pop();
                    }
                    pop();
                    super.endElement(uri, localName, qName);
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
                    pop();
                    super.endElement(uri, localName, qName);
                    state = State.STEP;
                }
        }
        level--;
        lastWasEndElement = true;
    }

    private volatile boolean finished = false;

    @Override
    public void endDocument() throws SAXException {
        super.endDocument();
        tmpBuffer.flush(endElementEventStack[0], null);
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
        try {
            lock.lock();
            while (state != State.WAIT) {
                try {
                    stateIsWait.await();
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
            }
            return finished;
        } finally {
            lock.unlock();
        }
    }

    private static final ContentHandler devNullContentHandler = new DevNullContentHandler();

    @Override
    public void skipOutput() {
        if (state != State.WAIT) {
            throw new IllegalStateException("expected state WAIT, found: "+state);
        }
        if (level == 0 || id == null) {
            throw new IllegalStateException();
        }
        refLevel = level;
        state = State.SKIP;
        setContentHandler(devNullContentHandler);
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
        StatefulXMLFilter sxf = new StatefulXMLFilter(10);
        sxf.setParent(spf.newSAXParser().getXMLReader());
        Thread t = new ParseThread(sxf);
        t.start();
        ContentHandler out = new EchoingContentHandler();
        while (!sxf.isFinished()) {
            if (!sxf.self()) {
                do {
                    System.out.println("\nXXXA"+sxf.getId());
                    System.out.print(sxf.buffersToString(10));
                    sxf.step();
                } while (!sxf.isFinished() && !sxf.self());
                if (!sxf.isFinished()) {
                    System.out.println("XXXB"+sxf.getId());
                    System.out.print(sxf.buffersToString(10));
                    sxf.writeOutput(out);
                }
            } else {
                System.out.println("\nXXXC" + sxf.getId());
                System.out.print(sxf.buffersToString(10));
                sxf.writeOutput(out);
            }
        }
        System.out.println("done!");
        System.out.print(sxf.buffersToString(10));
    }

    private static class ParseThread extends Thread {

        private final StatefulXMLFilter sxf;

        private ParseThread(StatefulXMLFilter sxf) {
            this.sxf = sxf;
        }

        @Override
        public void run() {
            try {
                sxf.parse("./src/test/resources/input/marcEmpty.xml");
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
        writingOutput = true;
        state = State.PLAY;
        try {
            lock.lock();
            stateNotWait.signal();
            if (!finished) {
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

    private volatile boolean writingOutput = false;

    @Override
    public void writeRootElement(ContentHandler ch) throws SAXException {
        startElementEventStack[0].play(ch, null);
        endElementEventStack[0].play(ch, null);
    }

    /**
     * Only ever to be called after having written some (self) output; this will
     * ensure that selfLevel will always be set appropriately.
     * @param ch
     * @param lowerLevel
     * @throws SAXException
     */
    @Override
    public void writeEndElements(ContentHandler ch, int lowerLevel, boolean aggregating) throws SAXException {
        if (debugging && selfLevel < 0) {
            throw new IllegalStateException("selfLevel not yet initialized!");
        }
        for (int i = (aggregating ? selfLevel - 1 : selfLevel); i >= lowerLevel; i--) {
            endElementEventStack[i].play(ch, null);
        }
    }

    /**
     * Only ever to be called when on self level. 
     * @param lowerLevel the level in the startElement stack from which to start
     * writing startElements
     * @param aggregate if true, writes the <code>currentLevel - 1</code> startElement
     * as self, and drops the <code>currentLevel</code> (self) startElement.
     */
    @Override
    public void writeStartElements(ContentHandler ch, int lowerLevel, boolean aggregating) throws SAXException {
        if (debugging) {
            if (state != State.WAIT) {
                throw new IllegalStateException("expected state WAIT, found: " + state);
            }
            if (!selfId) {
                throw new IllegalStateException("writeStartElements may only be called when cursor on selfId level");
            }
        }
        if (aggregating) {
            int aggregateSelfLevel = level - 1;
            if (aggregateSelfLevel >= lowerLevel) {
                for (int i = lowerLevel; i < aggregateSelfLevel; i++) {
                    startElementEventStack[i].play(ch, null);
                }
                startElementEventStack[aggregateSelfLevel].writeWithFinalElementSelfAttribute(ch, null, true);
            }
        } else {
            for (int i = lowerLevel; i <= level; i++) {
                startElementEventStack[i].play(ch, null);
            }
        }
    }

    private final UnboundedContentHandlerBuffer[] startElementEventStack;
    private final UnboundedContentHandlerBuffer[] endElementEventStack;

    private void push() throws SAXException {
        UnboundedContentHandlerBuffer tmp;
        if (lastWasEndElement) {
            tmp = endElementEventStack[level];
            endElementEventStack[level] = tmpBuffer;
            startElementEventStack[level].clear();
        } else {
            tmp = startElementEventStack[level];
            startElementEventStack[level] = tmpBuffer;
        }
        tmpBuffer = tmp;
        tmpBuffer.clear();
        setContentHandler(tmpBuffer);
    }

    private void pop() {
        if (lastWasEndElement) {
            if (writingOutput) {
                UnboundedContentHandlerBuffer tmp = endElementEventStack[level + 1];
                endElementEventStack[level + 1] = tmpBuffer;
                tmpBuffer = tmp;
            }
            tmpBuffer.clear();
        }
        setContentHandler(tmpBuffer);
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
