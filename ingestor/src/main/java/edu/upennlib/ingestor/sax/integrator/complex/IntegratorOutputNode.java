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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import org.apache.log4j.Appender;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.TTCCLayout;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;

/**
 *
 * @author michael
 */
public class IntegratorOutputNode implements Runnable {

    private final StatefulXMLFilter payload;
    private final boolean aggregateNode;
    private HashMap<String, IntegratorOutputNode> children = new HashMap<String, IntegratorOutputNode>();
    public static enum State {POTENTIALLY_WRITABLE, WRITABLE, NOT_WRITABLE}
    private State state = State.POTENTIALLY_WRITABLE;

    protected Logger logger = Logger.getLogger(getClass());

    @Override
    public void run() {
        if (!isAggregateNode() ^ children.isEmpty()) {
            throw new IllegalStateException();
        }
        if (!isAggregateNode()) {
            payload.run();
        } else {
            for (Entry<String, IntegratorOutputNode> e : children.entrySet()) {
                Thread t = new Thread(e.getValue(), e.getKey());
                t.start();
            }
            while (true) {
                blockUntilAtRest();
                logger.trace("point 1");
                if (childrenReadyToWrite()) {
                    logger.trace("point 2");
                    try {
                        writeChildOutputToPayload();
                    } catch (SAXException ex) {
                        throw new RuntimeException(ex);
                    }
                    logger.trace("point 3");
                } else {
                    logger.trace("point 4");
                    Comparable nextId = getLeastId();
                    logger.trace("point 5 "+nextId);
                    prepare(nextId);
                    logger.trace("point 6");
                }
            }
        }
    }

    public void blockUntilAtRest() {
        if (!isAggregateNode()) {
            payload.blockUntilAtRest();
        } else {
            for (IntegratorOutputNode child : children.values()) {
                child.blockUntilAtRest();
            }
        }
    }

    public IntegratorOutputNode(StatefulXMLFilter payload) {
        Appender appender = new ConsoleAppender(new TTCCLayout(), "System.out");
        logger.addAppender(appender);
        logger.setLevel(Level.TRACE);
        if (payload != null) {
            aggregateNode = false;
            this.payload = payload;
        } else {
            aggregateNode = true;
            this.payload = new StatefulXMLFilter();
        }
    }

    public boolean isAggregateNode() {
        return aggregateNode;
    }

    public void resetState() {
        state = State.POTENTIALLY_WRITABLE;
    }

    boolean xxxRequireForWrite;

    public void prepare(Comparable id) {
        if (isAggregateNode()) {
            if (state == State.POTENTIALLY_WRITABLE) {
                Iterator<IntegratorOutputNode> iter = children.values().iterator();
                state = State.WRITABLE;
                while (iter.hasNext() && state != State.NOT_WRITABLE) {
                    IntegratorOutputNode ion = iter.next();
                    ion.prepare(id);
                    switch (ion.state) {
                        case WRITABLE:
                            break;
                        case NOT_WRITABLE:
                            if (xxxRequireForWrite) {
                                state = State.NOT_WRITABLE;
                            }
                            break;
                        case POTENTIALLY_WRITABLE:
                            state = State.POTENTIALLY_WRITABLE;
                            break;
                    }
                }
            }
        } else {
            if (state == State.POTENTIALLY_WRITABLE) {
                Comparable localId = getId();
                if (id != null && localId != null && id.compareTo(localId) != 0) {
                    logger.trace("setting NOT_WRITABLE; id="+id+", localId="+localId);
                    state = State.NOT_WRITABLE;
                } else if (self()) {
                    logger.trace("setting WRITABLE");
                    state = State.WRITABLE;
                }
            }
        }
        switch (state) {
            case POTENTIALLY_WRITABLE:
                payload.setState(StatefulXMLFilter.State.STEP);
                synchronized (payload) {
                    logger.trace("1 notifying " + payload);
                    payload.notify();
                    try {
                        payload.wait();
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                }
                break;
            case WRITABLE:
                payload.setState(StatefulXMLFilter.State.PLAY);
                break;
            case NOT_WRITABLE:
                payload.setState(StatefulXMLFilter.State.SKIP);
                synchronized(payload) {
                    logger.trace("1 notifying "+payload);
                    payload.notify();
                    try {
                        payload.wait();
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                }
        }
    }

    private boolean writableStateDetermined() {
        return (state == State.WRITABLE || state == State.NOT_WRITABLE);
    }
    
    public boolean isWritable() {
        return state == State.WRITABLE;
    }

    // Called from parent.
    public void writeOutput(ContentHandler ch) throws SAXException {
        payload.writeOutput(ch);
    }


    private boolean childrenReadyToWrite() {
        for (IntegratorOutputNode child : children.values()) {
            if (!child.writableStateDetermined()) {
                return false;
            }
        }
        return true;
    }

    boolean xxxChildWritable = true;

    private AttributesImpl attRunner = new AttributesImpl();
    private void writeChildOutputToPayload() throws SAXException {
        for (Entry<String, IntegratorOutputNode> e : children.entrySet()) {
            if (xxxChildWritable) {
                payload.startElement("", e.getKey(), e.getKey(), attRunner);
                e.getValue().writeOutput(payload);
                payload.endElement("", e.getKey(), e.getKey());
            }
        }
    }

    public boolean self() {
        return payload.self();
    }

    public Comparable getId() {
        return payload.getId();
    }

    private Comparable getLeastId() {
        if (!isAggregateNode()) {
            throw new IllegalStateException();
        } else {
            Comparable id = null;
            for (IntegratorOutputNode ion : children.values()) {
                if (ion.state == State.POTENTIALLY_WRITABLE || ion.state == State.WRITABLE) {
                    Comparable tmpId = ion.getParentId();
                    if (tmpId != null && (id == null || tmpId.compareTo(id) < 0)) {
                        id = tmpId;
                    }
                }
            }
            return id;
        }
    }

    // Called from parent, not from self.
    public Comparable getParentId() {
        return payload.getParentId();
    }

    public void addChild(String childElementName, IntegratorOutputNode child) {
        children.put(childElementName, child);
    }
}
