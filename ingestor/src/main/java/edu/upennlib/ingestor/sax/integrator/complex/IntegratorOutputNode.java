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
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;
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
    private HashMap<String, IntegratorOutputNode> children;
    public static enum State {POTENTIALLY_WRITABLE, WRITABLE, NOT_WRITABLE}
    private State state = State.POTENTIALLY_WRITABLE;

    @Override
    public void run() {
        if (!isAggregateNode() ^ children.isEmpty()) {
            throw new IllegalStateException();
        }
        for (Entry<String,IntegratorOutputNode> e : children.entrySet()) {
            Thread t = new Thread(e.getValue(), e.getKey());
            t.start();
        }
        Thread t = new Thread(payload);
        t.start();
        while (true) {
            blockUntilAtRest();
            if (readyToWrite()) {
                try {
                    writeOutput(payload);
                } catch (SAXException ex) {
                    throw new RuntimeException(ex);
                }
            }
            Comparable nextId = getLeastId();
            prepare(nextId);
        }
    }

    public void blockUntilAtRest() {
        for (IntegratorOutputNode child : children.values()) {
            blockUntilAtRest();
        }
        payload.blockUntilAtRest();
    }

    public IntegratorOutputNode(StatefulXMLFilter payload) {
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

    public void updatePayloadState() {
        if (payload != null) {
            payload.updateState(state == State.POTENTIALLY_WRITABLE || state == State.WRITABLE);
        }
    }

    public void resetState() {
        state = State.POTENTIALLY_WRITABLE;
    }

    public void prepare(Comparable id) {
        if (state == State.POTENTIALLY_WRITABLE) {
            if (getId() == null || id.compareTo(getId()) != 0) {
                state = State.NOT_WRITABLE;
            } else if (self()) {
                state = State.WRITABLE;
            }
        }
        switch (state) {
            case POTENTIALLY_WRITABLE:
                updatePayloadState();
            case WRITABLE:
                for (IntegratorOutputNode ion : children.values()) {
                    ion.prepare(id);
                }
        }
    }

    public boolean readyToWrite() {
        if (state == State.POTENTIALLY_WRITABLE) {
            return false;
        } else if (state == State.WRITABLE) {
            for (IntegratorOutputNode ion : children.values()) {
                if (!ion.readyToWrite()) {
                    return false;
                }
            }
        }
        return true;
    }
    private AttributesImpl attRunner = new AttributesImpl();

    public void writeOutput(ContentHandler ch) throws SAXException {
        if (payload != null) {
            payload.writeOutput();
        }
        for (Entry<String, IntegratorOutputNode> e : children.entrySet()) {
            ch.startElement("uri", e.getKey(), "qName", attRunner);
            e.getValue().writeOutput(ch);
            ch.endElement("uri", e.getKey(), "qName");
        }
    }

    public boolean self() {
        return payload.self();
    }

    public Comparable getId() {
        return payload.getId();
    }

    public Comparable getLeastId() {
        if (!isAggregateNode()) {
            return getId();
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

    public Comparable getParentId() {
        if (payload != null) {
            return payload.getParentId();
        } else {
            return getId();
        }
    }

}
